#! /usr/bin/env sh
set -e

exec gunicorn -w 2 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:7772 app.main:app
