#!/bin/bash

while getopts ":n:p:" opt; do
    echo ${getopts}
    case ${opt} in
        n)
            ENV_NAME=${OPTARG}
            ;;
        p)
            PYTHON_VERSION=${OPTARG}
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

sudo apt-get install jq

conda create -n $ENV_NAME -y python=$PYTHON_VERSION jupyter ipython notebook ipykernel pandas numpy dask[dataframe] google-cloud-bigquery google-cloud-aiplatform pyarrow pandas-gbq openpyxl tabulate 

conda activate $ENV_NAME
conda install -y ipykernel pyarrow
# Add virtual environment as a kernel to Jupyter
ipython kernel install --user --name=$ENV_NAME --display-name="Python ($ENV_NAME)" 
conda deactivate

# File name
JSON_FILE="/opt/conda/envs/$ENV_NAME/share/jupyter/kernels/python3/kernel.json"
# Parameter to modify
PARAMETER_NAME="display_name"
# New value
NEW_VALUE="Python ($ENV_NAME)"

# Check if the JSON file exists
if [ ! -f "$JSON_FILE" ]; then
    echo "JSON file not found!"
    exit 1
fi

# Update the JSON file
jq --arg newValue "$NEW_VALUE" ".${PARAMETER_NAME} = \$newValue" $JSON_FILE > temp.json && mv temp.json $JSON_FILE

echo "Updated $PARAMETER_NAME in $JSON_FILE to $NEW_VALUE"

conda activate $ENV_NAME
conda deactivate

# Inform the user
echo "Virtual environment '$ENV_NAME' created and added to Jupyter kernels."
echo "To use it, start Jupyter Notebook and select 'Python ($ENV_NAME)' kernel."
