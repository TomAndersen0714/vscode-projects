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
    echo "${command_type_jobs}" | tr ' ' '\n' | xargs grep -l ${pattern} | sort -u | while read -r file; do
        # echo the lines
        echo -e "type=command, --type=spark, ${file}"
    done >./spark_jobs

    # get --type=hive jobs
    pattern="type=hive"
    echo "${command_type_jobs}" | tr ' ' '\n' | xargs grep -l ${pattern} | sort -u | while read -r file; do
        # echo the lines
        echo -e "type=command, --type=hive, ${file}"
    done >./hive_jobs
}

# get etlTool.py and etl_submit.sh jobs
get_all_client_jobs() {
    # get etlTool.py jobs
    pattern="etlTool.py|HAML_PYTHON_ETL_TOOL"
    echo "${command_type_jobs}" | tr ' ' '\n' | xargs grep -lE ${pattern} | sort -u | while read -r file; do
        # echo the lines
        echo -e "type=command, client=etlTool.py, ${file}"
    done >./etlTool_jobs

    # get etl_submit.sh jobs
    pattern="ETL_SUBMIT"
    echo "${command_type_jobs}" | tr ' ' '\n' | xargs grep -lE ${pattern} | sort -u | while read -r file; do
        # echo the lines
        echo -e "type=command, client=etl_submit.sh, ${file}"
    done >./etl_submit_jobs
}

# parse all command type jobs
parse_command_type_jobs() {
    # declare a map
    declare -A class_patterns
    class_patterns["subtype"]="type=spark type=hive"
    class_patterns["client"]="etlTool.py|HAML_PYTHON_ETL_TOOL ETL_SUBMIT"

    echo "${command_type_jobs}" | tr ' ' '\n' | xargs sort -u | while read -r file; do
        # result
        res="type=command"

        # iterate the map and match the pattern
        for key in "${!class_patterns[@]}"; do
            pattern=${class_patterns[${key}]}

            for p in ${pattern}; do
                # if match, then store the result and break
                if grep -qE "${p}" "${file}"; then
                    res+=", ${key}=${p}"
                    break
                fi
            done
        done

        # echo the lines
        res+=", ${file}"
        echo "${res}"
    done >./command_type_jobs
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
command_type_jobs=$(echo "$jobs" | tr ' ' '\n' | xargs grep -l 'type=command' | sort -u)

# execute MODEtions depend on MODE
case "${MODE}" in
1)
    get_hive_and_spark_type_jobs
    ;;
2)
    get_all_client_jobs
    ;;
3)
    parse_command_type_jobs
    ;;
*)
    echo "No implementation for MODE=${MODE}"
    ;;
esac
