// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Protocols;
using Microsoft.AspNetCore.Sockets.Client.Http;
using Microsoft.AspNetCore.Sockets.Client.Internal;
using Microsoft.AspNetCore.Sockets.Http.Internal;
using Microsoft.AspNetCore.Sockets.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;

namespace Microsoft.AspNetCore.Sockets.Client
{
    public partial class HttpConnection : IConnection
    {
        private static readonly TimeSpan HttpClientTimeout = TimeSpan.FromSeconds(120);

        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;

        private readonly SemaphoreSlim _stateLock = new SemaphoreSlim(1, 1);
        private bool _disposed = false;
        private bool _started = false;

        private IDuplexPipe _transportPipe;
        private IDuplexPipe _applicationPipe;

        private readonly HttpClient _httpClient;
        private readonly HttpOptions _httpOptions;
        private ITransport _transport;
        private Task _receiveLoopTask;
        private TaskCompletionSource<object> _startTcs;
        private TaskCompletionSource<object> _closeTcs;
        private TaskQueue _eventQueue;
        private readonly ITransportFactory _transportFactory;
        private string _connectionId;
        private Exception _abortException;
        private readonly TimeSpan _eventQueueDrainTimeout = TimeSpan.FromSeconds(5);
        private PipeReader Input => _transportPipe.Input;
        private PipeWriter Output => _transportPipe.Output;
        private readonly List<ReceiveCallback> _callbacks = new List<ReceiveCallback>();
        private readonly TransportType _requestedTransportType = TransportType.All;
        private readonly ConnectionLogScope _logScope;
        private readonly IDisposable _scopeDisposable;

        public Uri Url { get; }

        public IFeatureCollection Features { get; } = new FeatureCollection();

        public event Action<IConnection, Exception> Closed;

        public HttpConnection(Uri url)
            : this(url, TransportType.All)
        { }

        public HttpConnection(Uri url, TransportType transportType)
            : this(url, transportType, loggerFactory: null)
        {
        }

        public HttpConnection(Uri url, ILoggerFactory loggerFactory)
            : this(url, TransportType.All, loggerFactory, httpOptions: null)
        {
        }

        public HttpConnection(Uri url, TransportType transportType, ILoggerFactory loggerFactory)
            : this(url, transportType, loggerFactory, httpOptions: null)
        {
        }

        public HttpConnection(Uri url, TransportType transportType, ILoggerFactory loggerFactory, HttpOptions httpOptions)
        {
            Url = url ?? throw new ArgumentNullException(nameof(url));

            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = _loggerFactory.CreateLogger<HttpConnection>();
            _httpOptions = httpOptions;

            _requestedTransportType = transportType;
            if (_requestedTransportType != TransportType.WebSockets)
            {
                _httpClient = CreateHttpClient();
            }

            _transportFactory = new DefaultTransportFactory(transportType, _loggerFactory, _httpClient, httpOptions);
            _logScope = new ConnectionLogScope();
            _scopeDisposable = _logger.BeginScope(_logScope);
        }

        public HttpConnection(Uri url, ITransportFactory transportFactory, ILoggerFactory loggerFactory, HttpOptions httpOptions)
        {
            Url = url ?? throw new ArgumentNullException(nameof(url));
            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = _loggerFactory.CreateLogger<HttpConnection>();
            _httpOptions = httpOptions;
            _httpClient = CreateHttpClient();
            _transportFactory = transportFactory ?? throw new ArgumentNullException(nameof(transportFactory));
            _logScope = new ConnectionLogScope();
            _scopeDisposable = _logger.BeginScope(_logScope);
        }

        public Task StartAsync() => StartAsync(TransferFormat.Binary);

        public async Task StartAsync(TransferFormat transferFormat) => await StartAsyncCore(transferFormat).ForceAsync();

        private async Task<NegotiationResponse> GetNegotiationResponse()
        {
            var negotiationResponse = await Negotiate(Url, _httpClient, _logger);
            _connectionId = negotiationResponse.ConnectionId;
            _logScope.ConnectionId = _connectionId;
            return negotiationResponse;
        }

        private async Task StartAsyncCore(TransferFormat transferFormat)
        {
            CheckDisposed();

            await _stateLock.WaitAsync();

            try
            {
                CheckDisposed();
                if (!_started)
                {
                    throw new InvalidOperationException($"The '{nameof(StartAsync)}' method cannot be called if the connection has already been started.");
                }

                _startTcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                _eventQueue = new TaskQueue();

                Log.HttpConnectionStarting(_logger);

                await SelectAndStartTransport(transferFormat);

                // Start receiving
                _receiveLoopTask = ReceiveAsync();

                // Wire up to the Pipe's OnWriterCompleted, which indicates the transport completed (because the server closed).
                // We dump the task from our OnWriterCompleted method because OnWriterCompleted is void-returning.
                // Also, @pakrym says that if the pipe is already completed at the time we call this, the callback will still be run.
                Input.OnWriterCompleted((exception, state) => _ = OnWriterCompleted(exception), null);
            }
            finally
            {
                _stateLock.Release();
            }
        }

        private async Task OnWriterCompleted(Exception exception)
        {
            // We're done, mark as disposed.
            _disposed = true;

            Log.ProcessRemainingMessages(_logger);
            await _receiveLoopTask;

            Log.DrainEvents(_logger);
            await Task.WhenAny(_eventQueue.Drain().NoThrow(), Task.Delay(_eventQueueDrainTimeout));

            Log.CompleteClosed(_logger);
            _logScope.ConnectionId = null;

            try
            {
                Closed?.Invoke(this, exception);
            }
            catch (Exception ex)
            {
                // Suppress (but log) the exception, this is user code
                Log.ErrorDuringClosedEvent(_logger, ex);
            }
        }

        private async Task SelectAndStartTransport(TransferFormat transferFormat)
        {
            if (_requestedTransportType == TransportType.WebSockets)
            {
                Log.StartingTransport(_logger, _requestedTransportType, connectUrl);
                await StartTransport(Url, _requestedTransportType, transferFormat);
            }
            else
            {
                var negotiationResponse = await GetNegotiationResponse();

                // Connection is being disposed while start was in progress
                CheckDisposed();

                // This should only need to happen once
                var connectUrl = CreateConnectUrl(Url, negotiationResponse.ConnectionId);

                // We're going to search for the transfer format as a string because we don't want to parse
                // all the transfer formats in the negotiation response, and we want to allow transfer formats
                // we don't understand in the negotiate response.
                var transferFormatString = transferFormat.ToString();

                foreach (var transport in negotiationResponse.AvailableTransports)
                {
                    // Don't keep falling back if we're disposed while trying to connect.
                    CheckDisposed();

                    if (!Enum.TryParse<TransportType>(transport.Transport, out var transportType))
                    {
                        Log.TransportNotSupported(_logger, transport.Transport);
                        continue;
                    }

                    try
                    {
                        if ((transportType & _requestedTransportType) == 0)
                        {
                            Log.TransportDisabledByClient(_logger, transportType);
                        }
                        else if (!transport.TransferFormats.Contains(transferFormatString, StringComparer.Ordinal))
                        {
                            Log.TransportDoesNotSupportTransferFormat(_logger, transportType, transferFormat);
                        }
                        else
                        {
                            // The negotiation response gets cleared in the fallback scenario.
                            if (negotiationResponse == null)
                            {
                                negotiationResponse = await GetNegotiationResponse();
                                connectUrl = CreateConnectUrl(Url, negotiationResponse.ConnectionId);
                            }

                            Log.StartingTransport(_logger, transportType, connectUrl);
                            await StartTransport(connectUrl, transportType, transferFormat);
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.TransportFailed(_logger, transportType, ex);
                        // Try the next transport
                        // Clear the negotiation response so we know to re-negotiate.
                        negotiationResponse = null;
                    }
                }
            }

            if (_transport == null)
            {
                throw new InvalidOperationException("Unable to connect to the server with any of the available transports.");
            }
        }

        private async Task<NegotiationResponse> Negotiate(Uri url, HttpClient httpClient, ILogger logger)
        {
            try
            {
                // Get a connection ID from the server
                Log.EstablishingConnection(logger, url);
                var urlBuilder = new UriBuilder(url);
                if (!urlBuilder.Path.EndsWith("/"))
                {
                    urlBuilder.Path += "/";
                }
                urlBuilder.Path += "negotiate";

                using (var request = new HttpRequestMessage(HttpMethod.Post, urlBuilder.Uri))
                {
                    // Corefx changed the default version and High Sierra curlhandler tries to upgrade request
                    request.Version = new Version(1, 1);
                    SendUtils.PrepareHttpRequest(request, _httpOptions);

                    using (var response = await httpClient.SendAsync(request))
                    {
                        response.EnsureSuccessStatusCode();
                        return await ParseNegotiateResponse(response, logger);
                    }
                }
            }
            catch (Exception ex)
            {
                Log.ErrorWithNegotiation(logger, url, ex);
                throw;
            }
        }

        private static async Task<NegotiationResponse> ParseNegotiateResponse(HttpResponseMessage response, ILogger logger)
        {
            NegotiationResponse negotiationResponse;
            using (var reader = new JsonTextReader(new StreamReader(await response.Content.ReadAsStreamAsync())))
            {
                try
                {
                    negotiationResponse = new JsonSerializer().Deserialize<NegotiationResponse>(reader);
                }
                catch (Exception ex)
                {
                    throw new FormatException("Invalid negotiation response received.", ex);
                }
            }

            if (negotiationResponse == null)
            {
                throw new FormatException("Invalid negotiation response received.");
            }

            return negotiationResponse;
        }

        private static Uri CreateConnectUrl(Uri url, string connectionId)
        {
            if (string.IsNullOrWhiteSpace(connectionId))
            {
                throw new FormatException("Invalid connection id.");
            }

            return Utils.AppendQueryString(url, "id=" + connectionId);
        }

        private async Task StartTransport(Uri connectUrl, TransportType transportType, TransferFormat transferFormat)
        {
            var options = new PipeOptions(writerScheduler: PipeScheduler.Inline, readerScheduler: PipeScheduler.ThreadPool, useSynchronizationContext: false);
            var pair = DuplexPipe.CreateConnectionPair(options, options);
            _transportPipe = pair.Transport;
            _applicationPipe = pair.Application;
            _transport = _transportFactory.CreateTransport(transportType);

            // Start the transport, giving it one end of the pipeline
            try
            {
                await _transport.StartAsync(connectUrl, pair.Application, transferFormat, this);
            }
            catch (Exception ex)
            {
                Log.ErrorStartingTransport(_logger, _transport, ex);
                _transport = null;
                throw;
            }
        }

        private async Task ReceiveAsync()
        {
            try
            {
                Log.HttpReceiveStarted(_logger);

                while (true)
                {
                    if (_disposed.IsCancellationRequested)
                    {
                        Log.SkipRaisingReceiveEvent(_logger);

                        break;
                    }

                    var result = await Input.ReadAsync(_disposed.Token);
                    var buffer = result.Buffer;

                    try
                    {
                        if (!buffer.IsEmpty)
                        {
                            Log.ScheduleReceiveEvent(_logger);
                            var data = buffer.ToArray();

                            _ = _eventQueue.Enqueue(async () =>
                            {
                                Log.RaiseReceiveEvent(_logger);

                                // Copying the callbacks to avoid concurrency issues
                                ReceiveCallback[] callbackCopies;
                                lock (_callbacks)
                                {
                                    callbackCopies = new ReceiveCallback[_callbacks.Count];
                                    _callbacks.CopyTo(callbackCopies);
                                }

                                foreach (var callbackObject in callbackCopies)
                                {
                                    try
                                    {
                                        await callbackObject.InvokeAsync(data);
                                    }
                                    catch (Exception ex)
                                    {
                                        Log.ExceptionThrownFromCallback(_logger, nameof(OnReceived), ex);
                                    }
                                }
                            });

                        }
                        else if (result.IsCompleted)
                        {
                            break;
                        }
                    }
                    finally
                    {
                        Input.AdvanceTo(buffer.End);
                    }
                }
            }
            catch(OperationCanceledException)
            {
                // We've been disposed, just shut down.
            }
            catch (Exception ex)
            {
                Input.Complete(ex);

                Log.ErrorReceiving(_logger, ex);
            }
            finally
            {
                Input.Complete();
            }

            Log.EndReceive(_logger);
        }

        public async Task SendAsync(byte[] data, CancellationToken cancellationToken = default) =>
            await SendAsyncCore(data, cancellationToken).ForceAsync();

        private async Task SendAsyncCore(byte[] data, CancellationToken cancellationToken)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            if (_connectionState != ConnectionState.Connected)
            {
                throw new InvalidOperationException(
                    "Cannot send messages when the connection is not in the Connected state.");
            }

            Log.SendingMessage(_logger);

            cancellationToken.ThrowIfCancellationRequested();

            await Output.WriteAsync(data);
        }

        public Task AbortAsync(Exception exception)
        {
            // Simulate an error from the transport side by completing it.
            _applicationPipe.Output.Complete(exception);
            return Task.CompletedTask;
        }

        public async Task StopAsync() => await StopAsyncCore(exception: null).ForceAsync();

        private async Task StopAsyncCore(Exception exception)
        {
            lock (_stateChangeLock)
            {
                if (!(_connectionState == ConnectionState.Connecting || _connectionState == ConnectionState.Connected))
                {
                    Log.SkippingStop(_logger);
                    return;
                }
            }

            // Note that this method can be called at the same time when the connection is being closed from the server
            // side due to an error. We are resilient to this since we merely try to close the channel here and the
            // channel can be closed only once. As a result the continuation that does actual job and raises the Closed
            // event runs always only once.

            Log.StoppingClient(_logger);

            try
            {
                await _startTcs.Task;
            }
            catch
            {
                // We only await the start task to make sure that StartAsync completed. The
                // _startTask is returned to the user and they should handle exceptions.
            }

            TaskCompletionSource<object> closeTcs = null;
            Task receiveLoopTask = null;
            ITransport transport = null;

            lock (_stateChangeLock)
            {
                // Copy locals in lock to prevent a race when the server closes the connection and StopAsync is called
                // at the same time
                if (_connectionState != ConnectionState.Connected)
                {
                    // If not Connected then someone else disconnected while StopAsync was in progress, we can now NO-OP
                    return;
                }

                // Create locals of relevant member variables to prevent a race when Closed event triggers a connect
                // while StopAsync is still running
                closeTcs = _closeTcs;
                receiveLoopTask = _receiveLoopTask;
                transport = _transport;
            }

            if (_transportPipe != null)
            {
                Output.Complete();
            }

            if (transport != null)
            {
                await transport.StopAsync();
            }

            if (receiveLoopTask != null)
            {
                await receiveLoopTask;
            }

            if (closeTcs != null)
            {
                await closeTcs.Task;
            }
        }

        public async Task DisposeAsync() => await DisposeAsyncCore().ForceAsync();

        private async Task DisposeAsyncCore()
        {
            // This will no-op if we're already stopped
            await StopAsyncCore(exception: null);

            if (ChangeState(to: ConnectionState.Disposed) == ConnectionState.Disposed)
            {
                Log.SkippingDispose(_logger);

                // the connection was already disposed
                return;
            }

            Log.DisposingClient(_logger);

            _httpClient?.Dispose();
            _scopeDisposable.Dispose();
        }

        public IDisposable OnReceived(Func<byte[], object, Task> callback, object state)
        {
            var receiveCallback = new ReceiveCallback(callback, state);
            lock (_callbacks)
            {
                _callbacks.Add(receiveCallback);
            }
            return new Subscription(receiveCallback, _callbacks);
        }

        private class ReceiveCallback
        {
            private readonly Func<byte[], object, Task> _callback;
            private readonly object _state;

            public ReceiveCallback(Func<byte[], object, Task> callback, object state)
            {
                _callback = callback;
                _state = state;
            }

            public Task InvokeAsync(byte[] data)
            {
                return _callback(data, _state);
            }
        }

        private class Subscription : IDisposable
        {
            private readonly ReceiveCallback _receiveCallback;
            private readonly List<ReceiveCallback> _callbacks;
            public Subscription(ReceiveCallback callback, List<ReceiveCallback> callbacks)
            {
                _receiveCallback = callback;
                _callbacks = callbacks;
            }

            public void Dispose()
            {
                lock (_callbacks)
                {
                    _callbacks.Remove(_receiveCallback);
                }
            }
        }

        private ConnectionState ChangeState(ConnectionState from, ConnectionState to)
        {
            lock (_stateChangeLock)
            {
                var state = _connectionState;
                if (_connectionState == from)
                {
                    _connectionState = to;
                }

                Log.ConnectionStateChanged(_logger, state, to);
                return state;
            }
        }

        private ConnectionState ChangeState(ConnectionState to)
        {
            lock (_stateChangeLock)
            {
                var state = _connectionState;
                _connectionState = to;
                Log.ConnectionStateChanged(_logger, state, to);
                return state;
            }
        }

        private HttpClient CreateHttpClient()
        {
            HttpMessageHandler httpMessageHandler = null;
            if (_httpOptions != null)
            {
                var httpClientHandler = new HttpClientHandler();
                if (_httpOptions.Proxy != null)
                {
                    httpClientHandler.Proxy = _httpOptions.Proxy;
                }
                if (_httpOptions.Cookies != null)
                {
                    httpClientHandler.CookieContainer = _httpOptions.Cookies;
                }
                if (_httpOptions.ClientCertificates != null)
                {
                    httpClientHandler.ClientCertificates.AddRange(_httpOptions.ClientCertificates);
                }
                if (_httpOptions.UseDefaultCredentials != null)
                {
                    httpClientHandler.UseDefaultCredentials = _httpOptions.UseDefaultCredentials.Value;
                }
                if (_httpOptions.Credentials != null)
                {
                    httpClientHandler.Credentials = _httpOptions.Credentials;
                }

                httpMessageHandler = httpClientHandler;
                if (_httpOptions.HttpMessageHandler != null)
                {
                    httpMessageHandler = _httpOptions.HttpMessageHandler(httpClientHandler);
                    if (httpMessageHandler == null)
                    {
                        throw new InvalidOperationException("Configured HttpMessageHandler did not return a value.");
                    }
                }
            }

            var httpClient = httpMessageHandler == null ? new HttpClient() : new HttpClient(httpMessageHandler);
            httpClient.Timeout = HttpClientTimeout;

            return httpClient;
        }

        private void CheckDisposed()
        {
            if (_disposed.IsCancellationRequested)
            {
                throw new ObjectDisposedException(nameof(HttpConnection));
            }
        }

        // Internal because it's used by logging to avoid ToStringing prematurely.
        internal enum ConnectionState
        {
            Disconnected,
            Connected,
            Disposed
        }

        private class NegotiationResponse
        {
            public string ConnectionId { get; set; }
            public AvailableTransport[] AvailableTransports { get; set; }
        }

        private class AvailableTransport
        {
            public string Transport { get; set; }
            public string[] TransferFormats { get; set; }
        }
    }
}
