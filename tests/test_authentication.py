"""
Unit tests for Snowflake authentication configuration validation.
Tests that the target properly handles password and private key authentication.
"""
import pytest
from unittest.mock import patch, MagicMock
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from target_snowflake import main


def generate_test_private_key_pem():
    """A fresh RSA key in PKCS#8 PEM form, the shape a real snowflake_private_key config value has.

    Generated per test rather than checked in so the repository never carries a private key.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode('utf-8')


class TestAuthenticationConfiguration:
    """Test authentication configuration validation and logic."""

    def test_password_authentication_accepted(self):
        """Test that password authentication works correctly."""
        config = {
            'snowflake_account': 'test_account',
            'snowflake_warehouse': 'test_warehouse',
            'snowflake_database': 'test_db',
            'snowflake_username': 'test_user',
            'snowflake_password': 'test_password',
            'snowflake_schema': 'test_schema',
        }

        # Mock both connect and target_tools.main to avoid actual execution
        with patch('target_snowflake.connect') as mock_connect, \
             patch('target_snowflake.target_tools.main') as mock_target_main:

            mock_connection = MagicMock()
            mock_connect.return_value.__enter__.return_value = mock_connection

            # Call main
            main(config, input_stream=[])

            # Verify connect was called with password
            mock_connect.assert_called_once()
            call_kwargs = mock_connect.call_args[1]
            assert 'password' in call_kwargs
            assert call_kwargs['password'] == 'test_password'
            assert call_kwargs['authenticator'] == 'snowflake'
            assert 'private_key' not in call_kwargs

    def test_private_key_authentication_accepted(self):
        """Test that private key authentication works correctly."""
        test_private_key_pem = generate_test_private_key_pem()

        config = {
            'snowflake_account': 'test_account',
            'snowflake_warehouse': 'test_warehouse',
            'snowflake_database': 'test_db',
            'snowflake_username': 'test_user',
            'snowflake_private_key': test_private_key_pem,
            'snowflake_schema': 'test_schema',
        }

        # Mock both connect and target_tools.main to avoid actual execution
        with patch('target_snowflake.connect') as mock_connect, \
             patch('target_snowflake.target_tools.main') as mock_target_main:

            mock_connection = MagicMock()
            mock_connect.return_value.__enter__.return_value = mock_connection

            # Call main
            main(config, input_stream=[])

            # Verify connect was called with private_key as bytes
            mock_connect.assert_called_once()
            call_kwargs = mock_connect.call_args[1]
            assert 'private_key' in call_kwargs
            # The private_key should be bytes (DER format), not a string
            assert isinstance(call_kwargs['private_key'], bytes)
            assert call_kwargs['authenticator'] == 'SNOWFLAKE_JWT'
            assert 'password' not in call_kwargs

    def test_private_key_takes_precedence_over_password(self):
        """Test that private key is used when both password and private key are provided."""
        test_private_key_pem = generate_test_private_key_pem()

        config = {
            'snowflake_account': 'test_account',
            'snowflake_warehouse': 'test_warehouse',
            'snowflake_database': 'test_db',
            'snowflake_username': 'test_user',
            'snowflake_password': 'test_password',
            'snowflake_private_key': test_private_key_pem,
            'snowflake_schema': 'test_schema',
        }

        # Mock both connect and target_tools.main to avoid actual execution
        with patch('target_snowflake.connect') as mock_connect, \
             patch('target_snowflake.target_tools.main') as mock_target_main:

            mock_connection = MagicMock()
            mock_connect.return_value.__enter__.return_value = mock_connection

            # Call main
            main(config, input_stream=[])

            # Verify connect was called with private_key (as bytes), not password
            mock_connect.assert_called_once()
            call_kwargs = mock_connect.call_args[1]
            assert 'private_key' in call_kwargs
            assert isinstance(call_kwargs['private_key'], bytes)
            assert call_kwargs['authenticator'] == 'SNOWFLAKE_JWT'
            assert 'password' not in call_kwargs

    def test_no_authentication_raises_error(self):
        """Test that missing both password and private key raises an error."""
        config = {
            'snowflake_account': 'test_account',
            'snowflake_warehouse': 'test_warehouse',
            'snowflake_database': 'test_db',
            'snowflake_username': 'test_user',
            'snowflake_schema': 'test_schema',
        }

        # This should raise a ValueError
        with pytest.raises(ValueError) as exc_info:
            main(config, input_stream=[])

        assert 'Either snowflake_password or snowflake_private_key must be provided' in str(exc_info.value)

    def test_custom_authenticator_with_password(self):
        """Test that custom authenticator can be specified with password authentication."""
        config = {
            'snowflake_account': 'test_account',
            'snowflake_warehouse': 'test_warehouse',
            'snowflake_database': 'test_db',
            'snowflake_username': 'test_user',
            'snowflake_password': 'test_password',
            'snowflake_authenticator': 'externalbrowser',
            'snowflake_schema': 'test_schema',
        }

        # Mock both connect and target_tools.main to avoid actual execution
        with patch('target_snowflake.connect') as mock_connect, \
             patch('target_snowflake.target_tools.main') as mock_target_main:

            mock_connection = MagicMock()
            mock_connect.return_value.__enter__.return_value = mock_connection

            # Call main
            main(config, input_stream=[])

            # Verify connect was called with custom authenticator
            mock_connect.assert_called_once()
            call_kwargs = mock_connect.call_args[1]
            assert call_kwargs['authenticator'] == 'externalbrowser'
