import yaml

def read_formated_yaml_file(path):
    with open(path, 'r') as stream:
        try:
            credentials = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return credentials
