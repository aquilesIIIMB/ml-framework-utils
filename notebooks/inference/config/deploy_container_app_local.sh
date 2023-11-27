#!/bin/bash

while getopts ":m:p:a:" opt; do
    echo ${getopts}
    case ${opt} in
        m)
            MODEL_NAME=${OPTARG}
            ;;
        p)
            LOCAL_PORT=${OPTARG}
            ;;
        a)
            APP_PORT=${OPTARG}
            ;;
        \?)
            echo "Usage: cmd "
            exit 1
            ;;
        :)
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            exit 1
            ;;
    esac
done

repository=us-central1-docker.pkg.dev
project=ml-framework-maas
app_prefix=$(basename "$(dirname "$(dirname "$PWD")")")
project_prefix=$(basename "$(dirname "$(dirname "$(dirname "$(dirname "$PWD")")")")")
img_name=$(basename "$PWD")
version=latest

docker pull $repository/$project/$project_prefix/$app_prefix/$MODEL_NAME/$img_name:$version
docker run --publish $LOCAL_PORT:$APP_PORT $repository/$project/$project_prefix/$app_prefix/$MODEL_NAME/$img_name --app_port=$APP_PORT
