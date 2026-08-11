#!/bin/sh
set -e

mkdir -p "$DAGSTER_HOME"
cp /opt/dagster/config/dagster.yaml "$DAGSTER_HOME/dagster.yaml"
cp /opt/dagster/config/workspace.yml "$DAGSTER_HOME/workspace.yml"

exec "$@"
