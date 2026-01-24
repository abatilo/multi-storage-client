#!/bin/bash

usage() {
    cat <<EOF
Usage: $(basename "$0") [{ais|aisMinio|minio}"]

Populate the "dev" bucket in the specified object store (defaults to minio) with contents of the current directory tree.

Examples:
    $(basename "$0") -h          # Show this help message
    $(basename "$0") ais         # Populates the AIStore object store "dev" bucket
    $(basename "$0") aisMinio    # Populates the MinIO object store "dev" bucket cached by the AIStore object store "dev" bucket
    $(basename "$0") minio       # Populates the MinIO object store "dev" bucket
    $(basename "$0")             # Populates the (default) MinIO object store "dev" bucket
EOF
}

waitForAIStore() {
    aisCount="0"
    while [ "$aisCount" -ne 2 ]; do
        sleep 1

        (ais show cluster smap > /tmp/show_cluster_smap.out) || true
        aisCount=$(grep -c "http://ais:" /tmp/show_cluster_smap.out)
    done
}

waitForMinio() {
    minioCount="0"
    while [ "$minioCount" -ne 1 ]; do
        sleep 1

        (curl -s -I http://minio:9000/minio/health/live > /tmp/curl_minio_health_live.out) || true
        minioCount=$(grep -c "200 OK" /tmp/curl_minio_health_live.out)
    done
}

if [ $# -gt 1 ]; then
    usage
    exit
fi

if [ $# -eq 0 ]; then
    target_bucket=minio
else
    target_bucket=$1
fi

case "$target_bucket" in
    ais)
        waitForAIStore
        ais create ais://dev
        find . -type f | sed 's/^..//' | xargs -I {} ais put {} ais://dev/{}
        ais ls ais://dev
        ;;
    aisMinio)
        waitForMinio
        s3cmd mb s3://dev
        find . -type f | sed 's/^..//' | xargs -I {} s3cmd put {} s3://dev/{}
        s3cmd ls -r s3://dev
        waitForAIStore
        ais create s3://dev --skip-lookup
        ais bucket props set s3://dev features S3-Use-Path-Style
        # ais create ais://dev
        # ais bucket props set ais://dev backend_bck=s3://dev
        # ais ls ais://dev --all
        ais ls s3://dev --all
        ;;
    minio)
        waitForMinio
        s3cmd mb s3://dev
        find . -type f | sed 's/^..//' | xargs -I {} s3cmd put {} s3://dev/{}
        s3cmd ls -r s3://dev
        ;;
    *)
        usage
esac
