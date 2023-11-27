#!/bin/bash

while getopts ":a:" opt; do
    echo ${getopts}
    case ${opt} in
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


python ./src/main.py --app_port $APP_PORT
