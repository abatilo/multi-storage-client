#!/bin/bash

usage() {
    cat <<EOF
Usage: $(basename "$0") [-h] | [[[-i <iterations>] | [-t <seconds>]] [directory]...]

Generate read traffic across each specified directory (defaulting to /mnt/minio).

Examples:
    $(basename "$0") -h                      # Show this help message
    $(basename "$0")                         # Single pass across all files under /mnt/minio
    $(basename "$0") -i 21                   # Read all files under /mnt/minio 21 times
    $(basename "$0") -t 42                   # Repeatedly read all files under /mnt/minio for 42 seconds
    $(basename "$0") /mnt/ram /mnt/minio     # Single pass across all files under both /mnt/ram and /mnt/minio
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h)
                usage
                exit 0
                ;;
            -i)
                if [[ $# -lt 2 ]]; then
                    echo "Missing <iterations> value following -i option"
                    exit 1
                fi
                if [[ $2 -eq 0 ]]; then
                    echo "Found invalid <iterations> value of zero"
                    exit 1
                fi
                if [[ $NUM_ITERATIONS -ne 0 ]]; then
                    echo "Multiple -i <iterations> specified"
                    exit 1
                fi
                if [[ $NUM_SECONDS -ne 0 ]]; then
                    echo "Cannot specify both -i <iterations> and -t <seconds>"
                    exit 1
                fi
                NUM_ITERATIONS=$2
                shift 2
                ;;
            -t)
                if [[ $# -lt 2 ]]; then
                    echo "Missing <seconds> value following -t option"
                    exit 1
                fi
                if [[ $2 -eq 0 ]]; then
                    echo "Found invalid <seconds> value of zero"
                    exit 1
                fi
                if [[ $NUM_SECONDS -ne 0 ]]; then
                    echo "Multiple -t <seconds> specified"
                    exit 1
                fi
                if [[ $NUM_ITERATIONS -ne 0 ]]; then
                    echo "Cannot specify both -i <iterations> and -t <seconds>"
                    exit 1
                fi
                NUM_SECONDS=$2
                shift 2
                ;;
            *)
                break
        esac
    done

    if [[ $# -eq 0 ]]; then
        if [ -d "/mnt/minio" ]; then
            DIRECTORIES=("/mnt/minio")
        else
            echo "Missing default /mnt/minio directory"
            exit 1
        fi
    else
        while [[ $# -gt 0 ]]; do
            case "$1" in
                -h)
                    usage
                    exit 0
                    ;;
                *)
                    if [ -d $1 ]; then
                        DIRECTORIES+=("$1")
                        shift 1
                    else
                        echo "Missing directory: $1"
                        exit 1
                    fi
                    ;;
            esac
        done
    fi
}

NUM_ITERATIONS=0
NUM_SECONDS=0

declare -a DIRECTORIES

parse_args "$@"

if [[ $NUM_ITERATIONS -eq 0 && $NUM_SECONDS -eq 0 ]]; then
    NUM_ITERATIONS=1
fi

NUM_ITERATIONS_COMPLETED=0
START_TIME=$(date +%s)

while true; do
    (find ${DIRECTORIES[*]} -type f -exec md5sum {} \;) >> /dev/null
    NUM_ITERATIONS_COMPLETED=$((NUM_ITERATIONS_COMPLETED + 1))
    if [[ $NUM_ITERATIONS -ne 0 ]]; then
        if [[ $NUM_ITERATIONS_COMPLETED -ge $NUM_ITERATIONS ]]; then
            exit 0
        fi
    else
        CUR_TIME=$(date +%s)
        DELTA_TIME=$((CUR_TIME - START_TIME))
        if [[ $DELTA_TIME -ge $NUM_SECONDS ]]; then
            exit 0
        fi
    fi
done
