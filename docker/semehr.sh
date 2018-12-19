#!/usr/bin/env bash

datapath=/data
sesetting="$datapath/semehr_settings.json"
if [ ! -f "$sesetting" ]; then
    echo "setting file not found at/data/emehr_settings.json, use default settings in SemEHR repo"
    sesetting="$semehr_path/docker/docker_doc_based_settings.json"
fi

#check input docs
ndocs=`find $datapath/input_docs/ -maxdepth 1 -type f | wc -l`
if [[ $ndocs = 0 ]]; then
    echo 'no files found, copying sample docs...'
    cp "$semehr_path/resources/sample_docs/*" /data/input_docs
else
    echo "total $ndocs docs to process..."
fi
python "$semehr_path/semehr_processor.py" "$sesetting"
