#!/usr/bin/env bash

# exit when fails
set -e
# debug mode
export PS4='+${LINENO}: '
set -x

# get hive and spark jobs
get_hive_and_spark_type_jobs() {
    # get --type=spark jobs
    pattern="type=spark"
    echo "${command_type_jobs}" | xargs -n1 grep -l ${pattern} | sort | uniq | while read -r file; do
        # echo the lines
        echo -e "type=command, --type=spark, ${file}"
    done >./spark_jobs

    # get --type=hive jobs
    pattern="type=hive"
    echo "${command_type_jobs}" | xargs -n1 grep -l ${pattern} | sort | uniq | while read -r file; do
        # echo the lines
        echo -e "type=command, --type=hive, ${file}"
    done >./hive_jobs
}

# get etlTool.py and etl_submit.sh jobs
get_all_client_jobs() {
    # get etlTool.py jobs
    pattern="etlTool.py|HAML_PYTHON_ETL_TOOL"
    echo "${command_type_jobs}" | xargs -n1 grep -lE ${pattern} | sort | uniq | while read -r file; do
        # echo the lines
        echo -e "type=command, client=etlTool.py, ${file}"
    done >./etlTool_jobs

    # get etl_submit.sh jobs
    pattern="ETL_SUBMIT"
    echo "${command_type_jobs}" | xargs -n1 grep -lE ${pattern} | sort | uniq | while read -r file; do
        # echo the lines
        echo -e "type=command, client=etl_submit.sh, ${file}"
    done >./etl_submit_jobs
}

# parameters
JOBS_DIR=$1
MODE=$2

if [[ -z "${JOBS_DIR}" ]]; then
    echo "JOBS_DIR is empty"
    exit 1
fi

if [[ -z "${MODE}" ]]; then
    echo "MODE is empty"
    exit 1
fi

# get type=command jobs
jobs=$(find "${JOBS_DIR}" -name "*.job")
command_type_jobs=$(echo "$jobs" | xargs -n1 grep -l 'type=command' | sort | uniq)

# execute MODEtions depend on MODE
case "${MODE}" in
1)
    get_hive_and_spark_type_jobs
    ;;
2)
    get_all_client_jobs
    ;;
*)
    echo "No implementation for MODE=${MODE}"
    ;;
esac

# main