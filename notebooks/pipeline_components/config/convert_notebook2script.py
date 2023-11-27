import json
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--notebook_name',
        help='Notebook name to convert in Python Scripts',
        type=str,
        required=True
    )
    args = parser.parse_args()
    
    data = json.load(open(f'{args.notebook_name}.ipynb'))

    python_script_file = open("./src/main.py", "w")

    for cell in data['cells']:
        if cell['cell_type']=='code' and cell['source']:
            if "# input-data-definition (DON'T REMOVE THIS COMMENT)" in cell['source'][0]:
                python_script_file.write(''.join(cell['source']))
                python_script_file.write('\n\n')
            if "# input-data-ingestion (DON'T REMOVE THIS COMMENT)" in cell['source'][0]:
                python_script_file.write(''.join(cell['source']))
                python_script_file.write('\n\n')
            if "# process (DON'T REMOVE THIS COMMENT)" in cell['source'][0]:
                python_script_file.write(''.join(cell['source']))
                python_script_file.write('\n\n')
            if "# output-data-storing (DON'T REMOVE THIS COMMENT)" in cell['source'][0]:
                python_script_file.write(''.join(cell['source']))
                python_script_file.write('\n\n')
        if cell['cell_type']=='raw' and cell['source']:
            if "# step-execution (DON'T REMOVE THIS COMMENT)" in cell['source'][0]:
                python_script_file.write(''.join(cell['source']))
                python_script_file.write('\n\n')

    python_script_file.close()
    