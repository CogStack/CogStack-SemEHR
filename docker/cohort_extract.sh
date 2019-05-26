#!/usr/bin/env bash

COHORTCONF=/data/cohort.json

cd "$semehr_path"
#echo "update repo..."
#git pull

if [[ ! -f "$COHORTCONF" ]]; then
    echo "cohort configuration file not found at $COHORTCONF"
    exit -1
fi

echo "extracting docs using $COHORTCONF"
python cohort_helper.py "$COHORTCONF"
