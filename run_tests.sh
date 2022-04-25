#!/usr/bin/env sh

cd "$(dirname "$0")"
python3 -m unittest discover --top-level-directory=./cloud_pipelines/ --start-directory=./cloud_pipelines/_components/tests/ --verbose
