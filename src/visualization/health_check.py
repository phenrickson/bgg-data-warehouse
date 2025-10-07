"""Health check endpoint for Cloud Run."""

import os
import http.server
import socketserver
import threading
import time

# Get port from environment variable
PORT = int(os.environ.get("HEALTH_PORT", 8081))


class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    """Handler for health check requests."""

    def do_GET(self):
        """Handle GET requests."""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")

    def log_message(self, format, *args):
        """Suppress logging."""
        return


def start_health_check_server():
    """Start health check server in a separate thread."""
    handler = HealthCheckHandler
    httpd = socketserver.TCPServer(("", PORT), handler)
    print(f"Starting health check server on port {PORT}")

    # Run server in a separate thread
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    return httpd


if __name__ == "__main__":
    httpd = start_health_check_server()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        httpd.shutdown()
