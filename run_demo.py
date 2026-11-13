"""
CO3093 - One-Click Multi-Client Collaboration Demo Launcher.
Starts the central server and launches multiple clients with automated browser tabs.

Usage:
    python run_demo.py              # Launch default 2 clients (Alice on 5001 & Bob on 5002)
    python run_demo.py --clients 3  # Launch 3 clients (5001, 5002, 5003)
"""

import argparse
import subprocess
import sys
import time
import os
import webbrowser

DEMO_USERS = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"]


def main():
    parser = argparse.ArgumentParser(description="CO3093 Multi-Client Collaboration Demo Launcher")
    parser.add_argument("--clients", "-c", type=int, default=2, help="Number of clients to launch (2 to 6, default: 2)")
    args = parser.parse_args()

    num_clients = max(1, min(len(DEMO_USERS), args.clients))
    root_dir = os.path.abspath(os.path.dirname(__file__))

    server_proc = subprocess.Popen([sys.executable, "-m", "server.run_server"], cwd=root_dir)
    time.sleep(0.6)

    client_procs = []
    base_port = 5001

    for i in range(num_clients):
        username = DEMO_USERS[i]
        ui_port = base_port + i
        proc = subprocess.Popen(
            [sys.executable, "-m", "client.run_client", username, "--ui-port", str(ui_port), "--no-browser"],
            cwd=root_dir
        )
        client_procs.append(proc)
        time.sleep(0.3)

    time.sleep(0.5)
    for i in range(num_clients):
        try:
            webbrowser.open(f"http://127.0.0.1:{base_port + i}")
        except Exception:
            pass

    print("\n[Demo] Ready (Press Ctrl+C to stop).\n", flush=True)

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        for p in client_procs:
            try:
                p.terminate()
            except Exception:
                pass
        try:
            server_proc.terminate()
        except Exception:
            pass
        print("\n[Demo] Stopped.")


if __name__ == "__main__":
    main()
