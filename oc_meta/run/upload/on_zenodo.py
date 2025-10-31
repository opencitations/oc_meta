import argparse
import time
from pathlib import Path

import requests
import yaml
from tqdm import tqdm


class ProgressFileWrapper:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_size = Path(file_path).stat().st_size
        self.fp = open(file_path, 'rb')
        self.pbar = tqdm(total=self.file_size, unit='B', unit_scale=True, desc=Path(file_path).name)

    def read(self, size=-1):
        data = self.fp.read(size)
        self.pbar.update(len(data))
        return data

    def __len__(self):
        return self.file_size

    def close(self):
        self.fp.close()
        self.pbar.close()


def upload_file_with_retry(bucket_url, file_path, token, max_retries=5):
    filename = Path(file_path).name
    url = f"{bucket_url}/{filename}"

    for attempt in range(max_retries):
        try:
            print(f"\nAttempt {attempt + 1}/{max_retries}: {filename}")

            wrapper = ProgressFileWrapper(file_path)
            try:
                response = requests.put(
                    url,
                    data=wrapper,
                    headers={'Authorization': f'Bearer {token}'},
                    timeout=(30, 300)
                )
                response.raise_for_status()
            finally:
                wrapper.close()

            print(f"✓ {filename} uploaded successfully")
            return response

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"✗ Network error: {e}")
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.HTTPError as e:
            print(f"✗ HTTP error: {e}")
            raise


def main(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)

    sandbox = 'sandbox' in config['zenodo_url']
    base_url = 'https://sandbox.zenodo.org/api' if sandbox else 'https://zenodo.org/api'
    token = config['access_token']
    project_id = config['project_id']

    response = requests.get(
        f"{base_url}/deposit/depositions/{project_id}",
        headers={'Authorization': f'Bearer {token}'}
    )
    response.raise_for_status()
    bucket_url = response.json()['links']['bucket']

    print(f"Project ID: {project_id}")
    print(f"Bucket: {bucket_url}")
    print(f"Files to upload: {len(config['files'])}")

    for file_path in config['files']:
        upload_file_with_retry(bucket_url, file_path, token)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    args = parser.parse_args()
    main(args.config_file)