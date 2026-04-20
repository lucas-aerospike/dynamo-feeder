#!/usr/bin/env bash
#
# Launches the ItemFeeder personality of the dynamo-feeder JAR.
#
# Expects the following files to be present in the current working directory:
#   - dynamo-feeder-1.0-SNAPSHOT.jar
#   - lib/                          (transitive dependencies)
#
# All arguments passed to this script are forwarded to the Java process.

set -euo pipefail

JAR="dynamo-feeder-1.0-SNAPSHOT.jar"
LIB="lib"
MAIN_CLASS="com.aerospike.dynafeed.ItemFeeder"

if [[ ! -f "$JAR" ]]; then
    echo "ERROR: $JAR not found in current directory ($PWD)" >&2
    exit 1
fi

if [[ ! -d "$LIB" ]]; then
    echo "ERROR: $LIB/ directory not found in current directory ($PWD)" >&2
    exit 1
fi

exec java -cp "$JAR" "$MAIN_CLASS" "$@"
