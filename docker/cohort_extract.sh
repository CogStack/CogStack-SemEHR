#!/usr/bin/env bash

COHORTCONF=/data/cohort.json

if [[ ! -f "$COHORTCONF" ]]; then
    echo "cohort configuration file not found at $COHORTCONF"
    exit -1
fi

cd "$semehr_path"
echo "extracting docs using $COHORTCONF"
python cohort_helper.py "$COHORTCONF"