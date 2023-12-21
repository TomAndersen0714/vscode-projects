#!/usr/bin/env bash

# exit when fails
set -e
# debug mode
export PS4='+${LINENO}: '
set -x

# get all subtype
parse_all_jobs_types() {
    echo "${command_type_jobs}" | grep -l "bdp-client" | xargs grep -ohE '\-\-type=[a-zA-Z0-9_]+' | sort -u
    echo "${command_type_jobs}" | grep -l "bdp-client" | xargs grep -ohE '\-t [a-zA-Z0-9_]+' | sort -u
}

# parse all command type jobs
parse_command_type_jobs() {
    # declare a job type map
    declare -A class_patterns
    class_patterns["bdp_client"]="etl_submit.sh|ETL_SUBMIT,etl_submit_4_ira.sh|ETL_SUBMIT_4_IRA,etlTool.py|HAML_PYTHON_ETL_TOOL"
    class_patterns["bdp_job"]="blanca,spark,hive,etl,echo,checkDate,sqoop"

    # check pattern
    # grep -rlE "ETL_SUBMIT|etlTool.py|HAML_PYTHON_ETL_TOOL" | xargs grep -L "bdp-client"
    # grep -rlE "bdp-client" | xargs grep -LE "ETL_SUBMIT|etlTool.py|HAML_PYTHON_ETL_TOOL"

    # set split character
    IFS=','

    echo "${command_type_jobs}" | tr ' ' '\n' | sort -u | while read -r file_name; do
        # result to log
        res="wtss_job='command'"

        # iterate the map and match the patterns
        for key in "${!class_patterns[@]}"; do
            patterns=${class_patterns[${key}]}
            res+=", ${key}="
            match="false"

            for pattern in ${patterns}; do
                # if match, then store the result and break
                if grep -qEw "${pattern}" "${file_name}"; then
                    res+="'${pattern}'"
                    match="true"
                    break
                fi
            done

            # if not match
            if [ "${match}" == "false" ]; then
                res+="''"
            fi
        done

        # echo the lines
        res+=", job='${file_name}'"
        echo "${res}"
    done >./command_type_jobs

    # cancel split character setting
    unset IFS
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
    parse_command_type_jobs
    ;;
*)
    echo "No implementation for MODE=${MODE}"
    ;;
esac