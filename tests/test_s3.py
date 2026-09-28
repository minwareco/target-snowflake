import io
import re
from unittest.mock import patch

import pytest

from target_snowflake.s3 import S3


class FakeS3Client:
    """Keeps uploaded bytes so tests assert on what landed in S3, not on which boto3 call put it there."""

    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.read()

    def upload_fileobj(self, Fileobj, Bucket, Key):
        self.objects[(Bucket, Key)] = Fileobj.read()


@pytest.fixture
def fake_client():
    client = FakeS3Client()
    with patch('target_snowflake.s3.boto3.client', return_value=client) as factory:
        yield client, factory


def test_persist_uploads_utf8_bytes_under_prefixed_key(fake_client):
    client, _ = fake_client
    s3 = S3('AKIA', 'secret', 'bucket', 'base/')

    bucket, key = s3.persist(io.StringIO('a,b\n1,ü\n'), key_prefix='table__')

    assert bucket == 'bucket'
    assert re.fullmatch(r'base/table__[0-9a-f]{32}', key)
    assert client.objects[('bucket', key)] == 'a,b\n1,ü\n'.encode('utf-8')


class ChunkedReadable:
    """Mimics target_postgres.postgres.TransformStream, which is what persist_csv_rows passes in:
    each read() returns the next CSV line and '' once the batch is exhausted."""

    def __init__(self, lines):
        self._lines = iter(lines)

    def read(self, *args, **kwargs):
        return next(self._lines, '')


def test_persist_uploads_every_chunk_of_a_line_at_a_time_stream(fake_client):
    client, _ = fake_client
    s3 = S3('AKIA', 'secret', 'bucket')

    bucket, key = s3.persist(ChunkedReadable(['a,b\n', '1,2\n', '3,4\n']))

    assert client.objects[(bucket, key)] == b'a,b\n1,2\n3,4\n'


def test_persist_treats_missing_key_prefix_as_empty(fake_client):
    _, key = S3('AKIA', 'secret', 'bucket', None).persist(io.StringIO('x'))

    assert re.fullmatch(r'[0-9a-f]{32}', key)


def test_credentials_reach_the_client_and_are_exposed(fake_client):
    _, factory = fake_client

    s3 = S3('AKIA', 'secret', 'bucket', '', aws_session_token='tok')

    factory.assert_called_once_with(
        's3', aws_access_key_id='AKIA', aws_secret_access_key='secret', aws_session_token='tok')
    assert s3.credentials() == {
        'aws_access_key_id': 'AKIA', 'aws_secret_access_key': 'secret', 'aws_session_token': 'tok'}
