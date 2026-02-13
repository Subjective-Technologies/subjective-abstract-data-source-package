# File: SubjectiveRealTimeDataSource.py

import asyncio
import json
import logging
import threading
import time
from abc import abstractmethod
from .SubjectiveDataSource import SubjectiveDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger  # Ensure BBLogger is correctly implemented


class SubjectiveRealTimeDataSource(SubjectiveDataSource):
    # Internal host and port (not exposed to users)
    _HOST = 'localhost'
    _PORT = 65432  # Fixed port for simplicity; adjust as needed

    def __init__(self, name=None, session=None, dependency_data_sources=None, subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources,
            subscribers=subscribers,
            params=params
        )
        self._server = None
        self._connected_clients = set()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._monitoring_active = False
        self._monitoring_thread = None
        self._reconnect_enabled = True
        self._reconnect_initial_delay = self._coerce_float_param("reconnect_initial_delay", 1.0)
        self._reconnect_max_delay = self._coerce_float_param("reconnect_max_delay", 30.0)
        self._reconnect_jitter = self._coerce_float_param("reconnect_jitter", 0.2)
        self._reconnect_max_attempts = self._coerce_int_param("reconnect_max_attempts", -1)
        BBLogger.log("Asyncio event loop started in a separate thread.")

    def _coerce_float_param(self, key, default):
        try:
            value = self.params.get(key, default)
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _coerce_int_param(self, key, default):
        try:
            value = self.params.get(key, default)
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    def _get_reconnect_delay(self, attempt):
        # Exponential backoff with a small jitter to avoid synchronized retries.
        base_delay = min(self._reconnect_initial_delay * (2 ** max(attempt - 1, 0)), self._reconnect_max_delay)
        jitter = base_delay * self._reconnect_jitter
        return max(0.0, base_delay + (jitter * (0.5 - time.time() % 1)))

    def _should_retry(self, attempt):
        if not self._reconnect_enabled:
            return False
        if self._reconnect_max_attempts < 0:
            return True
        return attempt <= self._reconnect_max_attempts

    def _run_with_reconnect(self, connect_fn, run_fn, disconnect_fn=None, label="real-time stream"):
        """
        Run a real-time stream with automatic reconnects.

        connect_fn: callable invoked before run_fn for each attempt.
        run_fn: blocking callable that runs the stream until it ends or raises.
        disconnect_fn: optional callable to clean up the connection between attempts.
        """
        attempt = 0
        while self._monitoring_active:
            attempt += 1
            try:
                BBLogger.log(f"{label} - starting (attempt {attempt})")
                if connect_fn:
                    connect_fn()
                run_fn()
                if not self._monitoring_active:
                    break
                BBLogger.log(f"{label} - ended; preparing to reconnect")
            except Exception as e:
                if not self._monitoring_active:
                    break
                BBLogger.log(f"{label} - error: {e}")
            finally:
                if disconnect_fn:
                    try:
                        disconnect_fn()
                    except Exception as cleanup_err:
                        BBLogger.log(f"{label} - cleanup error: {cleanup_err}")

            if not self._should_retry(attempt):
                BBLogger.log(f"{label} - reconnect disabled or max attempts reached")
                break

            delay = self._get_reconnect_delay(attempt)
            if delay > 0:
                BBLogger.log(f"{label} - reconnecting in {delay:.2f}s")
                time.sleep(delay)

    def _run_loop(self):
        """Run the asyncio event loop."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _handle_client(self, reader, writer):
        """Handle incoming data connections."""
        addr = writer.get_extra_info('peername')
        BBLogger.log(f"Connection established from {addr}")
        self._connected_clients.add(writer)
        try:
            while True:
                data = await reader.readline()
                if not data:
                    BBLogger.log(f"Connection closed by {addr}")
                    break
                message = data.decode('utf-8').strip()
                if message:
                    try:
                        data_dict = json.loads(message)
                        BBLogger.log(f"Received data: {data_dict} from {addr}")
                        self.update(data_dict)  # Notify subscribers
                    except json.JSONDecodeError:
                        BBLogger.log(f"Invalid JSON received from {addr}: {message}")
        except asyncio.IncompleteReadError:
            BBLogger.log(f"Connection lost with {addr}")
        finally:
            self._connected_clients.remove(writer)
            writer.close()
            await writer.wait_closed()
            BBLogger.log(f"Connection with {addr} closed.")

    async def _start_server_async(self):
        """Start the asyncio server."""
        self._server = await asyncio.start_server(
            self._handle_client, self._HOST, self._PORT, loop=self._loop
        )
        addr = self._server.sockets[0].getsockname()
        BBLogger.log(f"Real-time Data Source Server listening on {addr}")
        async with self._server:
            await self._server.serve_forever()

    def _start_server(self):
        """Start the server coroutine."""
        if not self._server:
            asyncio.run_coroutine_threadsafe(self._start_server_async(), self._loop)
            BBLogger.log("Real-time Data Source Server started.")

    def _send_mock_data(self):
        """Send mock data to all subscribers."""
        mock_data = {
            "timestamp": "2025-01-08T12:00:00.000000",
            "value": "Mock data update after 10 seconds"
        }
        BBLogger.log("Sending mock data update to subscribers.")
        self.update(mock_data)  # Notify subscribers

    def _schedule_mock_update(self):
        """Schedule sending mock data after a 10-second delay."""
        timer = threading.Timer(10, self._send_mock_data)
        timer.start()
        BBLogger.log("Scheduled mock data update to be sent after 10 seconds.")

    # Enhanced Real-Time Data Source Methods
    
    def start_monitoring(self):
        """
        Start real-time monitoring. This method should be implemented by subclasses
        to define their specific monitoring behavior (file system, network, etc.).
        """
        if self._monitoring_active:
            BBLogger.log("Monitoring is already active")
            return

        BBLogger.log(f"Starting monitoring for {self.__class__.__name__}")
        self._monitoring_active = True

        # Start the monitoring implementation in a separate thread
        try:
            self._initialize_monitoring()

            # Run monitoring implementation in background thread
            self._monitoring_thread = threading.Thread(
                target=self._start_monitoring_implementation,
                daemon=True
            )
            self._monitoring_thread.start()

            BBLogger.log(f"Monitoring started successfully for {self.__class__.__name__}")
        except Exception as e:
            self._monitoring_active = False
            BBLogger.log(f"Failed to start monitoring: {e}")
            raise

    def stop_monitoring(self):
        """
        Stop real-time monitoring.
        """
        if not self._monitoring_active:
            BBLogger.log("Monitoring is not active")
            return
            
        BBLogger.log(f"Stopping monitoring for {self.__class__.__name__}")
        self._monitoring_active = False
        
        try:
            self._stop_monitoring_implementation()
            BBLogger.log(f"Monitoring stopped successfully for {self.__class__.__name__}")
        except Exception as e:
            BBLogger.log(f"Error while stopping monitoring: {e}")

    def _initialize_monitoring(self):
        """
        Initialize monitoring resources. Override in subclasses if needed.
        """
        pass

    def _start_monitoring_implementation(self):
        """
        Implementation-specific monitoring start. Override in subclasses.

        Subclasses can either:
        1. Override this method directly for simple monitoring (default behavior)
        2. Implement _connect_stream, _run_stream, and optionally _disconnect_stream
           methods, which will be automatically wrapped with reconnection logic.
        """
        # Check if subclass has implemented stream methods for reconnection
        has_connect = hasattr(self, '_connect_stream') and callable(getattr(self, '_connect_stream'))
        has_run = hasattr(self, '_run_stream') and callable(getattr(self, '_run_stream'))

        if has_connect and has_run:
            # Use reconnection wrapper for stream-based monitoring
            connect_fn = self._connect_stream
            run_fn = self._run_stream
            disconnect_fn = getattr(self, '_disconnect_stream', None) if hasattr(self, '_disconnect_stream') else None
            label = getattr(self, '_stream_label', f"{self.__class__.__name__} stream")

            self._run_with_reconnect(
                connect_fn=connect_fn,
                run_fn=run_fn,
                disconnect_fn=disconnect_fn,
                label=label
            )
        else:
            # Default implementation for non-stream monitoring
            self._start_server()
            self._schedule_mock_update()

    def _stop_monitoring_implementation(self):
        """
        Implementation-specific monitoring stop. Override in subclasses.
        Default implementation stops the TCP server.
        """
        self._stop_server()

    def send_notification(self, notification_data):
        """
        Send a notification to all subscribers with the provided data.
        This is the main method subclasses should use to send real-time updates.
        
        :param notification_data: Dictionary containing the notification data
        """
        try:
            # Add timestamp if not present
            if 'timestamp' not in notification_data:
                notification_data['timestamp'] = time.time()
                
            BBLogger.log(f"Sending real-time notification: {notification_data}")
            self.update(notification_data)  # Notify subscribers
            
        except Exception as e:
            BBLogger.log(f"Error sending notification: {e}")

    def send_redis_notification(self, channel, notification_data):
        """
        Send a notification via Redis channel (if Redis is available).
        
        :param channel: Redis channel name
        :param notification_data: Dictionary containing the notification data
        """
        try:
            import redis
            from com_subjective_utils.config import config
            
            # Create Redis connection
            redis_host = config.get('REDIS_SERVER_IP', 'localhost')
            redis_port = config.get('REDIS_SERVER_PORT', 6379)
            
            redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
            
            # Add timestamp if not present
            if 'timestamp' not in notification_data:
                notification_data['timestamp'] = time.time()
                
            # Publish to Redis channel
            message = json.dumps(notification_data)
            redis_client.publish(channel, message)
            
            BBLogger.log(f"Sent Redis notification to channel '{channel}': {notification_data}")
            
        except ImportError:
            BBLogger.log("Redis not available for notifications")
        except Exception as e:
            BBLogger.log(f"Error sending Redis notification: {e}")

    def subscribe(self, subscriber):
        """
        Subscribe a subscriber and initiate listening.

        :param subscriber: An instance of BBSubscriber to be notified.
        """
        super().subscribe(subscriber)
        # Start monitoring when first subscriber is added
        if len(self.subscribers) == 1:
            self.start_monitoring()

    def _stop_server(self):
        """Stop the server and close all connections."""
        if self._server:
            self._server.close()
            asyncio.run_coroutine_threadsafe(self._server.wait_closed(), self._loop)
            BBLogger.log("Real-time Data Source Server closing.")
        for writer in self._connected_clients:
            writer.close()
            asyncio.run_coroutine_threadsafe(writer.wait_closed(), self._loop)
            BBLogger.log("Closed connection with a client.")
        # Stop the event loop
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        BBLogger.log("Real-time Data Source Server stopped and event loop terminated.")

    def stop(self):
        """Public method to gracefully shut down the data source."""
        self.stop_monitoring()
        self._stop_server()

    def fetch(self):
        """
        Implement the abstract fetch method. For real-time data sources,
        this starts monitoring and keeps running until monitoring stops.
        """
        BBLogger.log(f"Fetch called on real-time data source {self.__class__.__name__}")
        self.start_monitoring()

        # Wait for the monitoring thread to complete (blocks until stop_monitoring is called)
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            try:
                self._monitoring_thread.join()
            except KeyboardInterrupt:
                BBLogger.log(f"Fetch interrupted, stopping monitoring")
                self.stop_monitoring()
