"""
Unit tests for Snowflake authentication configuration validation.
Tests that the target properly handles password and private key authentication.
"""
import pytest
from unittest.mock import patch, MagicMock

from target_snowflake import main


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
        # Valid RSA 2048-bit private key in PEM format for testing (generated for test only)
        test_private_key_pem = '''-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCn80L6Ookj51RS
op7maLIJneBv5bKUCwZiIDRkLbG+uTJ4FNg2O4RA+q5RHpLcoNl8rxT34qN7by5F
sfwmNoTFxEghUA38CT18M3hnbQmPx5QuzxNDETZG+85GrpVZqgdADv6zgKSlSmnx
61EGHXglfSBId8DOR93Cs/I1U+VgNJr72fh8X2j1PkH+lwBgPO/4/w0nPEVRc4P5
ZBaDWTRmCL2frZYvx6lvhWc+pYOzZQ4KUQgvRt39DWeqS8TpehOzL0zT6JOaf+Io
c49/X3+dUqVEwb+xJusoKN7MxJfxz/jFvHYjRsfwBcCXV8hRRze4Hf6CPcGdFfC3
3/FVO+3rAgMBAAECggEAEMmJQacQMNKH5H/0uhBvm0kjNza9FCcgoDZFMQOQPKwY
61UfYhcDhSs0DqUBBk8y7fo3clTpBQbRQo4j0f3+ZMuAb4lSRzBKlluJXBfTWNfD
bAUEETQV5Mp5N9TDgOQ0jVFHttF+Tjc97RHEVi7Oj5C45VrISYDNcw9hf7W/EPqH
piNII7Dg5lRaiwPRvsuOHxwYjtIQQtYce1LnXGVeVJxSmFqEhVErEXA10CClYkhR
givPNmQEKaVv0SisGoqL6hrCw4SbXt0fwuDlXbm8EeCQgzxe+3fJlCm48W20dHFO
fhGNtCFG0QAk4Q3hxpkxNcqyu1eIjqxqhz8nylSfIQKBgQDVMDcn1fHpnw7uo6G+
3atL6lxWtJlI/tJ5OuXwxtlV5X2P0E3NVLmt8b4UU46IGho46fa2Tp+IeYnbvo2x
8hGdwmnBMhaBo4IwSyNlBO+1LfylhK/v1II7tq1Yo2TgXvfYnbX/oxka7hMl2E7f
wC6Jf7NXgJe+e/LZd/c3QNvnlQKBgQDJrWdgZoFTs8WD1tPTaSRjtJzjxVTp5NoS
w7o5xcxOqZqvh3nAa72rxT66wPGQMl4EFNZ875YXVm5i3RTVnZOqVZkDLhBu5EPv
qbOuGfhld6XxAwtQ77XW8SvPTOlPLBK8MCbvKoC5DzkwCX3UVKQSxhkjGMyt5qKn
LnCN8HUffwKBgDi1Z6aQEZaceeNe4ZKc8ojyIXfq+G9jYWdgFHRU4NEph5nuxhNd
ezra+D398AciMmF7UuYxydwKwHIUoSp5gtgdM/ZxNW1sqh/gjNy9UGo4fmElB4vb
Un1B3aCbbiUE/ha/9P64SuBP/gXuISUBwR9QOcuH6FWCMRpKABfRh+11AoGACWts
+aawAa3S2t6M1EID7hhAf6720VncCaZUq2Aes8neLLaiLCecG0rCLEzYu4hutbgX
cIxsMTjbPQjgcT3D6N/InspnABbvSWFewBH8dRjKimA/Bg+8KYboKe2ItCb11Q5W
szMEAiDA5gp7cxBk/W99OxNsc+7ix/Y2UZrajZcCgYB1+Gsh0m6vodaC3QxiBz8Y
oGyny41cNPWZbUeoklHEX9Z0AKrtaFJPcEUS08jnBUpH2jdrrUdvh8p92tRg6Yg/
0k67zirAdkHbj/fxsHXn6VvMAxmjDYEolydYcZiyC4rbrks5ekk17P7RwqFS6Oca
kePnRegr03GHzqW5tZzDbA==
-----END PRIVATE KEY-----'''

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
        # Valid RSA 2048-bit private key in PEM format for testing (generated for test only)
        test_private_key_pem = '''-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCn80L6Ookj51RS
op7maLIJneBv5bKUCwZiIDRkLbG+uTJ4FNg2O4RA+q5RHpLcoNl8rxT34qN7by5F
sfwmNoTFxEghUA38CT18M3hnbQmPx5QuzxNDETZG+85GrpVZqgdADv6zgKSlSmnx
61EGHXglfSBId8DOR93Cs/I1U+VgNJr72fh8X2j1PkH+lwBgPO/4/w0nPEVRc4P5
ZBaDWTRmCL2frZYvx6lvhWc+pYOzZQ4KUQgvRt39DWeqS8TpehOzL0zT6JOaf+Io
c49/X3+dUqVEwb+xJusoKN7MxJfxz/jFvHYjRsfwBcCXV8hRRze4Hf6CPcGdFfC3
3/FVO+3rAgMBAAECggEAEMmJQacQMNKH5H/0uhBvm0kjNza9FCcgoDZFMQOQPKwY
61UfYhcDhSs0DqUBBk8y7fo3clTpBQbRQo4j0f3+ZMuAb4lSRzBKlluJXBfTWNfD
bAUEETQV5Mp5N9TDgOQ0jVFHttF+Tjc97RHEVi7Oj5C45VrISYDNcw9hf7W/EPqH
piNII7Dg5lRaiwPRvsuOHxwYjtIQQtYce1LnXGVeVJxSmFqEhVErEXA10CClYkhR
givPNmQEKaVv0SisGoqL6hrCw4SbXt0fwuDlXbm8EeCQgzxe+3fJlCm48W20dHFO
fhGNtCFG0QAk4Q3hxpkxNcqyu1eIjqxqhz8nylSfIQKBgQDVMDcn1fHpnw7uo6G+
3atL6lxWtJlI/tJ5OuXwxtlV5X2P0E3NVLmt8b4UU46IGho46fa2Tp+IeYnbvo2x
8hGdwmnBMhaBo4IwSyNlBO+1LfylhK/v1II7tq1Yo2TgXvfYnbX/oxka7hMl2E7f
wC6Jf7NXgJe+e/LZd/c3QNvnlQKBgQDJrWdgZoFTs8WD1tPTaSRjtJzjxVTp5NoS
w7o5xcxOqZqvh3nAa72rxT66wPGQMl4EFNZ875YXVm5i3RTVnZOqVZkDLhBu5EPv
qbOuGfhld6XxAwtQ77XW8SvPTOlPLBK8MCbvKoC5DzkwCX3UVKQSxhkjGMyt5qKn
LnCN8HUffwKBgDi1Z6aQEZaceeNe4ZKc8ojyIXfq+G9jYWdgFHRU4NEph5nuxhNd
ezra+D398AciMmF7UuYxydwKwHIUoSp5gtgdM/ZxNW1sqh/gjNy9UGo4fmElB4vb
Un1B3aCbbiUE/ha/9P64SuBP/gXuISUBwR9QOcuH6FWCMRpKABfRh+11AoGACWts
+aawAa3S2t6M1EID7hhAf6720VncCaZUq2Aes8neLLaiLCecG0rCLEzYu4hutbgX
cIxsMTjbPQjgcT3D6N/InspnABbvSWFewBH8dRjKimA/Bg+8KYboKe2ItCb11Q5W
szMEAiDA5gp7cxBk/W99OxNsc+7ix/Y2UZrajZcCgYB1+Gsh0m6vodaC3QxiBz8Y
oGyny41cNPWZbUeoklHEX9Z0AKrtaFJPcEUS08jnBUpH2jdrrUdvh8p92tRg6Yg/
0k67zirAdkHbj/fxsHXn6VvMAxmjDYEolydYcZiyC4rbrks5ekk17P7RwqFS6Oca
kePnRegr03GHzqW5tZzDbA==
-----END PRIVATE KEY-----'''

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
