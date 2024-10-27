#!/bin/bash

trap "echo 'Parse stopped'; exit" SIGINT

while true; do
    python collector.py

    if [ $? -eq 0 ]; then
        timestamp=$(date +"%d.%m.%Y_%H_%M")

        cp data.json archive/
        mv archive/data.json "archive/${timestamp}.json"

        echo "Data copied to archive/${timestamp}.json"

        python upload.py
    else
        echo "collector.py failed, skipping upload.py"
    fi

    sleep 5
done