import uuid

import boto3


class S3:
    """Uploads a CSV batch to S3 and reports where it landed, so Snowflake can COPY it from an
    external stage location. The credentials are kept because the COPY statement needs them too.
    """

    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket, key_prefix='',
                 aws_session_token=None):
        self._credentials = {'aws_access_key_id': aws_access_key_id,
                             'aws_secret_access_key': aws_secret_access_key,
                             'aws_session_token': aws_session_token}
        self.client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token)
        self.bucket = bucket
        # `target_s3.key_prefix` is optional in the config, so None means no prefix
        self.key_prefix = key_prefix or ''

    def credentials(self):
        return self._credentials

    def persist(self, readable, key_prefix=''):
        key = self.key_prefix + key_prefix + uuid.uuid4().hex
        # persist_csv_rows passes a target_postgres TransformStream: each read() returns the next
        # CSV line and '' once the batch is exhausted, so drain it the same way the internal-stage
        # branch does. A batch is bounded by target-postgres's max_batch_size, so holding it in
        # memory for a single put_object is fine.
        chunks = []
        line = readable.read()
        while line:
            chunks.append(line.encode('utf-8'))
            line = readable.read()
        self.client.put_object(Bucket=self.bucket, Key=key, Body=b''.join(chunks))
        return [self.bucket, key]
