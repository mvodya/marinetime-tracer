#!/bin/bash

source /workspace/venv/bin/activate

trap "echo 'Parse stopped'; exit" SIGINT

# 25 min watchdog
timeout 1500 python collector.py

if [ $? -eq 0 ]; then
    timestamp=$(date +"%d.%m.%Y_%H_%M")

    cp data.json archive/
    mv archive/data.json "archive/${timestamp}.json"

    echo "Data copied to archive/${timestamp}.json"

    python upload.py
elif [ $? -eq 124 ]; then
    echo "collector.py timed out after 25 minutes"
else
    echo "collector.py failed, skipping upload.py"
fi
