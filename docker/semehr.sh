#!/usr/bin/env bash

sesetting=/data/semehr_settings.json
if [ ! -f "$sesetting" ]; then
    echo "setting file not found at/data/emehr_settings.json, use default settings in SemEHR repo"
    sesetting="$semehr_path/docker/docker_doc_based_settings.json"
fi
python "$semehr_path/semehr_processor.py" "$sesetting"
