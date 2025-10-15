#!/usr/bin/env bash
set -e
pip install -r requirements.txt
docker compose up -d
python postgres_replication.py
docker compose down