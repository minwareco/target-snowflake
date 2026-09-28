# Copied verbatim from target-redshift 0.2.4, target_redshift/s3.py
# (https://github.com/datamill-co/target-redshift, MIT License, Copyright 2018-2021 Data Mill
# Services, LLC). Only the module path changed.
#
# It lives here so the target can stay current on snowflake-connector-python: target-redshift pins
# boto3<1.10 and urllib3==1.25.9, while snowflake-connector-python 3.18.1 and newer require
# boto3>=1.24, so the two cannot be installed together. This helper was the only thing used from
# target-redshift, so carrying it in-repo removes that package and lets the connector move.
#
# persist_csv_rows passes a target_postgres TransformStream, whose read() returns one CSV line per
# call and '' at the end; _EncodeBinaryReadable drains it into bytes for upload_fileobj.

import uuid

import boto3

SEPARATOR = '__'


class S3:
    def __init__(
        self,
        aws_access_key_id,
        aws_secret_access_key,
        bucket,
        key_prefix='',
        aws_session_token=None
    ):
        self._credentials = {'aws_access_key_id': aws_access_key_id,
                             'aws_secret_access_key': aws_secret_access_key,
                             'aws_session_token': aws_session_token}
        self.client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token)
        self.bucket = bucket
        self.key_prefix = key_prefix

    def credentials(self):
        return self._credentials

    def persist(self, readable, key_prefix=''):
        key = self.key_prefix + key_prefix + str(uuid.uuid4()).replace('-', '')

        self.client.upload_fileobj(
            _EncodeBinaryReadable(readable),
            self.bucket,
            key)

        return [self.bucket, key]


class _EncodeBinaryReadable:
    def __init__(self, readable_obj):
        self.input = readable_obj

    def readable(self):
        return True

    def read(self, *args, **kwargs):
        if len(args) > 0:
            max_bytes = args[0]
        else:
            max_bytes = None
        output = b''
        while (max_bytes is not None and len(output) < max_bytes) or True:  ## TODO: overflow?
            line = self.input.read()
            if line == '':
                return output
            output += line.encode('utf-8')
        return output
