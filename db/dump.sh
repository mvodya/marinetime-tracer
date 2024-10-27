#!/bin/bash
PGPASSWORD=${POSTGRES_PASSWORD} pg_dump -h ${POSTGRES_HOST} -U ${POSTGRES_USER} --schema-only ${POSTGRES_DB} > schema.sql