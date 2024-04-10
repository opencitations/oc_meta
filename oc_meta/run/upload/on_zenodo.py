import argparse
import yaml
import zenodopy

def load_config(yaml_file_path):
    with open(yaml_file_path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return None

def main(config_file):
    # Load configuration
    config = load_config(config_file)
    if config is None:
        print("Failed to load the configuration.")
        return

    # Determine if we are using the sandbox environment
    sandbox = "sandbox" in config['zenodo_url']

    # Initialize zenodopy client
    zeno = zenodopy.Client(token=config['access_token'], sandbox=sandbox)

    project_id = config.get('project_id')
    if project_id:
        # Set the existing project
        zeno.set_project(project_id)
    else:
        # Create a new project (deposition)
        title = config.get('title', '')
        description = config.get('description', '')
        upload_type = config.get('upload_type', '')
        zeno.create_project(title=title, description=description, upload_type=upload_type)

    # Upload files to the project
    for file_path in config['files']:
        zeno.upload_file(file_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload files to Zenodo using a YAML configuration.")
    parser.add_argument('config_file', type=str, help="Path to the YAML configuration file.")
    args = parser.parse_args()

    main(args.config_file)