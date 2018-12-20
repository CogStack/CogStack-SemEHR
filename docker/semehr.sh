#!/usr/bin/env bash

datapath=/data
sesetting="$datapath/semehr_settings.json"
if [ ! -f "$sesetting" ]; then
    echo "setting file not found at/data/emehr_settings.json, use default settings in SemEHR repo"
    sesetting="$semehr_path/docker/docker_doc_based_settings.json"
fi

if [ ! -x "$datapath/input_docs" ]; then
    echo "creating input_docs folder"
    mkdir "$datapath/input_docs"
fi

if [ ! -x "$datapath/output_docs" ]; then
    echo "creating output_docs folder"
    mkdir "$datapath/output_docs"
fi

if [ ! -x "$datapath/semehr_results" ]; then
    echo "creating semehr_results folder"
    mkdir "$datapath/semehr_results"
fi

#check input docs
ndocs=`find $datapath/input_docs/ -maxdepth 1 -type f | wc -l`
if [[ $ndocs = 0 ]]; then
    echo 'no files found, copying sample docs...'
    cp "$semehr_path"/resources/sample_docs/* /data/input_docs
else
    echo "total $ndocs docs to process..."
fi

# update repo
cd "$semehr_path"
git pull

python "$semehr_path/semehr_processor.py" "$sesetting"
