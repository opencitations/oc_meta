import yaml
import argparse
from internetarchive import upload

def upload_files(config_path):
    # Load the configuration from the YAML file
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    # Extract data from the configuration file
    identifier = config['identifier']
    file_paths = config['file_paths']
    metadata = config['metadata']
    access_key = config['access_key']
    secret_key = config['secret_key']

    # Perform the upload
    result = upload(identifier=identifier, files=file_paths, metadata=metadata,
                    access_key=access_key, secret_key=secret_key)

    # Check the upload result and print appropriate message
    if result[0].status_code == 200:
        print("Upload completed successfully!")
    else:
        print("Upload failed.")

def main():
    parser = argparse.ArgumentParser(description="Upload files to the Internet Archive.")
    parser.add_argument('config_path', type=str, help="Path to the YAML configuration file.")
    args = parser.parse_args()

    upload_files(args.config_path)

if __name__ == '__main__':
    main()