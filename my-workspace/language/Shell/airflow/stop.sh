#!/bin/bash

AIRFLOW_HOME=~/airflow
PIDS=(
    'airflow-webserver.pid'
    'airflow-scheduler.pid'
    'airflow-worker.pid'
    'airflow-flower.pid'
)

cd $AIRFLOW_HOME || exit

for pid in "${PIDS[@]}"; do
    echo "$pid"
    xargs kill < "$pid"
    printf "%0.s-" {1..40} && echo
    echo
done
