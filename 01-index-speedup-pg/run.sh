#!/usr/bin/env bash
set -e
pip install -r requirements.txt
docker compose up -d
sleep 5
python index_speedup.py
docker compose down 