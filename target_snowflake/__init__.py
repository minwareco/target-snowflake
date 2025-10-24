import singer
from singer import utils
from target_postgres import target_tools
from target_redshift.s3 import S3

from target_snowflake.connection import connect
from target_snowflake.snowflake import SnowflakeTarget

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'snowflake_account',
    'snowflake_warehouse',
    'snowflake_database',
    'snowflake_username',
]


def main(config, input_stream=None):
    # Validate that we have either password or private_key for authentication
    has_password = config.get('snowflake_password')
    has_private_key = config.get('snowflake_private_key')

    if not has_password and not has_private_key:
        raise ValueError(
            'Either snowflake_password or snowflake_private_key must be provided for authentication'
        )

    # Build connection parameters
    connection_params = {
        'user': config.get('snowflake_username'),
        'role': config.get('snowflake_role'),
        'account': config.get('snowflake_account'),
        'warehouse': config.get('snowflake_warehouse'),
        'database': config.get('snowflake_database'),
        'schema': config.get('snowflake_schema', 'PUBLIC'),
        'autocommit': False,
        'client_session_keep_alive': True,
        # turn off OCSP checking to avoid disconnection in the case of very long running connections
        # why: https://www.snowflake.com/blog/latest-changes-to-how-snowflake-handles-ocsp/
        # doc: https://community.snowflake.com/s/article/How-to-turn-off-OCSP-checking-in-Snowflake-client-drivers
        'insecure_mode': True,
    }

    # Use private key authentication if available, otherwise fall back to password
    if has_private_key:
        connection_params['private_key'] = has_private_key
        connection_params['authenticator'] = 'SNOWFLAKE_JWT'
        LOGGER.info('Using private key authentication for Snowflake connection')
    else:
        connection_params['password'] = has_password
        connection_params['authenticator'] = config.get('snowflake_authenticator', 'snowflake')
        LOGGER.info('Using password authentication for Snowflake connection')

    with connect(**connection_params) as connection:
        s3_config = config.get('target_s3')

        s3 = None
        if s3_config:
            s3 = S3(s3_config.get('aws_access_key_id'),
                    s3_config.get('aws_secret_access_key'),
                    s3_config.get('bucket'),
                    s3_config.get('key_prefix'))

        target = SnowflakeTarget(
            connection,
            s3=s3,
            logging_level=config.get('logging_level'),
            persist_empty_tables=config.get('persist_empty_tables')
        )

        if input_stream:
            target_tools.stream_to_target(input_stream, target, config=config)
        else:
            target_tools.main(target)


def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
