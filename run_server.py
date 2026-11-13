"""
CO3093 - Collaboration Server Entry Point.
Usage:
    python run_server.py [--host 0.0.0.0] [--tcp-port 8888] [--udp-port 8889]
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from server.run_server import main

if __name__ == "__main__":
    main()
