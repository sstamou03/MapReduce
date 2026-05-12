import http.server
import socketserver
import json
import subprocess
import threading
import os
import webbrowser
import time
import sys
from queue import Queue

PORT = 9999
DASHBOARD_DIR = "dashboard"

# State management
port_forwards = {}
command_outputs = Queue()

class CommandHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DASHBOARD_DIR, **kwargs)

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data)
        action = data.get('action')

        response = {"status": "ok", "output": ""}

        if action == "run_command":
            cmd = data.get('command')
            # Run command in a separate thread to avoid blocking
            thread = threading.Thread(target=self.execute_async, args=(cmd,))
            thread.start()
            response["output"] = "Command started in background..."

        elif action == "start_port_forward":
            svc = data.get('service')
            local_port = data.get('local_port')
            remote_port = data.get('remote_port')
            if svc in port_forwards: port_forwards[svc].terminate()
            cmd = f"kubectl port-forward svc/{svc} {local_port}:{remote_port}"
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            port_forwards[svc] = proc
            response["output"] = f"Started port-forward for {svc}"

        elif action == "stop_port_forward":
            svc = data.get('service')
            if svc in port_forwards:
                port_forwards[svc].terminate()
                del port_forwards[svc]
            response["output"] = f"Stopped port-forward for {svc}"

        elif action == "get_pods":
            try:
                result = subprocess.run("kubectl get pods -o json", shell=True, capture_output=True, text=True)
                response["output"] = json.loads(result.stdout)
            except:
                response["status"] = "error"

        elif action == "minikube_status":
            try:
                result = subprocess.run("minikube status --format={{.Host}}", shell=True, capture_output=True, text=True)
                response["output"] = result.stdout.strip() or "Stopped"
            except:
                response["output"] = "Offline"

        elif action == "get_logs":
            # Return captured outputs
            logs = []
            while not command_outputs.empty():
                logs.append(command_outputs.get())
            response["output"] = "\n".join(logs)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

    def execute_async(self, cmd):
        print(f"[DEBUG] Executing command: {cmd}")
        try:
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            for line in process.stdout:
                print(f"[CMD OUTPUT] {line.strip()}")
                command_outputs.put(line)
            process.stdout.close()
            return_code = process.wait()
            print(f"[DEBUG] Command finished with code: {return_code}")
            command_outputs.put(f"--- Command Finished (Code: {return_code}) ---")
        except Exception as e:
            print(f"[DEBUG] Command Error: {str(e)}")
            command_outputs.put(f"Error: {str(e)}")

def start_server():
    # Use ThreadingMixIn to handle multiple requests
    class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
        pass

    with ThreadedHTTPServer(("", PORT), CommandHandler) as httpd:
        print(f"Server started at http://localhost:{PORT}")
        url = f"http://localhost:{PORT}"
        try:
            chrome_path = "C:/Program Files/Google/Chrome/Application/chrome.exe"
            if os.path.exists(chrome_path):
                subprocess.Popen([chrome_path, f"--app={url}"])
            else:
                webbrowser.open(url)
        except:
            webbrowser.open(url)
        httpd.serve_forever()

if __name__ == "__main__":
    if not os.path.exists(DASHBOARD_DIR): os.makedirs(DASHBOARD_DIR)
    start_server()
