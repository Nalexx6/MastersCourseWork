import argparse
import boto3
import os
import zipfile
import logging

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


def zipdir(path):
    folder_name = os.path.basename(path)

    with zipfile.ZipFile(f'{folder_name}.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(path):
            for file in files:
                if '__pycache__' not in (os.path.join(root, file)):
                    zipf.write(os.path.join(root, file),
                               os.path.relpath(os.path.join(root, file),
                                               os.path.join(path, '..')))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='aws_params_parser',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument('--s3-bucket', required=True)
    parser.add_argument('--s3-object-path', required=True)

    args = parser.parse_args()
    bucket = args.s3_bucket
    object_path = args.s3_object_path

    zipdir('spark_apps')

    files = [
             './spark_apps/main.py',
             './deployment/bootstrap.sh',
             './deployment/script-runner.jar',
             './deployment/emr-submit-spark.sh',
             './spark_apps.zip']

    s3 = boto3.client(service_name='s3')

    for f in files:
        s3.upload_file(f, bucket, os.path.join(object_path, f'{os.path.basename(f)}'))
        logging.info(f'uploaded file {f}')

