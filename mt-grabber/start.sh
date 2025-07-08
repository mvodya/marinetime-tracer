#!/bin/bash
docker run --env-file $(pwd)/.env  -v $(pwd)/archive:/workspace/archive --rm mt-grabber