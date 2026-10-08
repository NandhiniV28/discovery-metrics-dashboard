"""
Local-only server for the Discovery Dashboard.

Serves dashboard.html and exposes:
  GET  /api/data        re-runs the Jira pipeline live and returns fresh JSON.
                         The Jira token never leaves this process - it is read
                         server-side from ~/.config/jira/token and is not sent
                         to the browser in any response.
  POST /api/save-order   persists a drag-and-drop reorder (order.json) and
                         commits + pushes it to the repo using this machine's
                         own git credentials, so the new order is shared with
                         everyone viewing the public dashboard. Only available
                         when running this server - the public GitHub Pages
                         site has no write credentials and is read-only.

Run:  python3 server.py
Then open http://localhost:8765/dashboard.html
"""
import json
import os
import subprocess
import sys
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse

import pipeline
import build as build_mod

PORT = 8765
DIRECTORY = os.path.dirname(os.path.abspath(__file__))


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/api/data":
            self._handle_api_data()
        elif path == "/":
            self.send_response(302)
            self.send_header("Location", "/dashboard.html")
            self.end_headers()
        else:
            super().do_GET()

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/api/save-order":
            self._handle_save_order()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_save_order(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            order = json.loads(body)
            if not isinstance(order, dict):
                raise ValueError("Expected a JSON object of {product: [TDI keys...]}")

            with open(os.path.join(DIRECTORY, "order.json"), "w") as f:
                json.dump(order, f, indent=2)

            # Bake the new order into the local data.json/dashboard.html right
            # away so the page reflects it without waiting for a Jira refresh.
            data_path = os.path.join(DIRECTORY, "data.json")
            with open(data_path) as f:
                data = json.load(f)
            data["custom_order"] = order
            with open(data_path, "w") as f:
                json.dump(data, f, indent=2)
            build_mod.build(data)

            print("Committing and pushing updated TDI order...", flush=True)
            subprocess.run(["git", "add", "order.json", "data.json", "dashboard.html", "index.html"],
                            cwd=DIRECTORY, check=True)
            diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=DIRECTORY)
            if diff.returncode != 0:  # there are staged changes
                subprocess.run(["git", "commit", "-m", "Update manual TDI order"], cwd=DIRECTORY, check=True)
                subprocess.run(["git", "push"], cwd=DIRECTORY, check=True)
                pushed = True
            else:
                pushed = False

            body_out = json.dumps({"ok": True, "pushed": pushed}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body_out)))
            self.end_headers()
            self.wfile.write(body_out)
        except Exception as e:
            print("Save order failed:", e, file=sys.stderr, flush=True)
            body_out = json.dumps({"ok": False, "error": str(e)}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body_out)))
            self.end_headers()
            self.wfile.write(body_out)

    def _handle_api_data(self):
        try:
            print("Refreshing from Jira...", flush=True)
            data = pipeline.run(progress=lambda m: print(" ", m, flush=True))
            body = json.dumps(data).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            # keep a local cache of the latest pull, and rebuild the static
            # dashboard.html so opening it directly (no server) also shows it
            with open(os.path.join(DIRECTORY, "data.json"), "w") as f:
                json.dump(data, f, indent=2)
            build_mod.build(data)
        except Exception as e:
            print("Refresh failed:", e, file=sys.stderr, flush=True)
            body = json.dumps({"error": str(e)}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f"[server] {self.address_string()} - {fmt % args}", flush=True)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("localhost", PORT), Handler)
    print(f"Discovery Dashboard server running at http://localhost:{PORT}/dashboard.html")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping.")
        server.shutdown()
