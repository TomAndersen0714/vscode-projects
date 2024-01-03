#!/usr/bin/env bash

set -ex

grep -rin 'type=command' | awk -F ':' '{print $1}' | xargs grep -rni 'type=spark' | awk -F ':' '{print $1}' | sort | uniq > ./spark_jobs
grep -rin 'type=command' | awk -F ':' '{print $1}' | xargs grep -rni 'type=hive' | awk -F ':' '{print $1}' | sort | uniq > ./hive_jobs
