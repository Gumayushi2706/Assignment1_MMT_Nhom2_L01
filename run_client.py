"""
CO3093 - Collaboration Client Entry Point.
Usage:
    python run_client.py Alice
    python run_client.py Bob --host 192.168.1.15
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from client.run_client import main

if __name__ == "__main__":
    main()
