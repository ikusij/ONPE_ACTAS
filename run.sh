#!/bin/bash
set -e

source venv/bin/activate

python fetch.py
python compute.py

git add .
git commit -m "update ($(date '+%Y-%m-%d %H:%M:%S'))"
git push
