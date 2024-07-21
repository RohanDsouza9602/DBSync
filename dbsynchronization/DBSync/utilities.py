import yaml

def load_yaml(file_name):
    try:
        with open(file_name, 'r') as file:
            data = yaml.safe_load(file)
            return data
    except Exception as e:
        print(f"Error loading YAML file {file_name}: {e}")
        return None