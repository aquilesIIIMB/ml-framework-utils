#!/bin/bash

while getopts ":m:p:d:" opt; do
    echo ${getopts}
    case ${opt} in
        m)
            MODEL_NAME=${OPTARG}
            ;;
        p)
            PYTHON_VERSION=${OPTARG}
            ;;
        d)
            CONTAINER_DESCRIPTION=${OPTARG}
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

gcloud artifacts repositories create $project_prefix --repository-format=docker --location=us-central1 --description="${CONTAINER_DESCRIPTION}" --labels=applicationName={{cookiecutter.applicationName}},gitProject={{cookiecutter.projectName}}
docker build --tag $repository/$project/$project_prefix/$app_prefix/$MODEL_NAME/$img_name:$version --build-arg PYTHON_VERSION=$PYTHON_VERSION .
docker image prune --force
docker push $repository/$project/$project_prefix/$app_prefix/$MODEL_NAME/$img_name:$version

echo "CONTAINER_IMAGE_URI:"
echo $repository/$project/$project_prefix/$app_prefix/$MODEL_NAME/$img_name:$version
