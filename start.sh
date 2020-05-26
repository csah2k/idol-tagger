#!/bin/bash
gunicorn --threads=20 --bind 0.0.0.0:8080 indexflow:app