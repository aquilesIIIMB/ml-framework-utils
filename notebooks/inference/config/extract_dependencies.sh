#!/bin/bash

pip install pipdeptree

pipdeptree --warn silence | grep -E '^\w+' > requirements.txt

# Define the function
remove_lines_containing_word() {
    if [ "$#" -ne 2 ]; then
        echo "Usage: remove_lines_containing_word <file_path> <word>"
        return 1
    fi

    # File name and word as arguments
    local FILE_NAME=$1
    local WORD=$2

    # Check if file exists
    if [ ! -f "$FILE_NAME" ]; then
        return 1
    fi

    # Remove lines containing the word
    sed -i "/$WORD/d" $FILE_NAME
}

# Remove GDAL lib of requirements.txt
remove_lines_containing_word requirements.txt GDAL
