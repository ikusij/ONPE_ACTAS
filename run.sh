#!/bin/bash
set -e

source venv/bin/activate

python fetch.py
python compute.py
