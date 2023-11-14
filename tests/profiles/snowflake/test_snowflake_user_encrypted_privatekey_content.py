"Tests for the Snowflake user/private_key_content profile."

import base64
import json
import pathlib
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection
from cryptography.hazmat.primitives import serialization

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.snowflake import (
    SnowflakeEncryptedPrivateKeyContentPemProfileMapping,
)


@pytest.fixture()
def mock_snowflake_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    conn = Connection(
        conn_id="my_snowflake_pk_connection",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        password="secret",
        extra=json.dumps(
            {
                "account": "my_account",
                "region": "my_region",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_content": "my_content",
            }
        ),
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


@pytest.fixture()
def mock_p8_snowflake_conn():  # type: ignore
    """
    Sets the connection as an environment variable.
    """
    p8_private_key = (
        pathlib.Path(__file__).parent
        / "test_snowflake_user_encrypted_privatekey_content.p8"
    )
    with open(p8_private_key, "r", encoding="utf-8") as private_key:
        private_key_content = private_key.read()
    conn = Connection(
        conn_id="my_snowflake_pk_connection",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        password="my_password",
        extra=json.dumps(
            {
                "account": "my_account",
                "region": "my_region",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_content": private_key_content,
            }
        ),
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Snowflake profile mapping claims the correct connection type.
    """
    potential_values = {
        "conn_type": "snowflake",
        "login": "my_user",
        "schema": "my_database",
        "password": "secret",
        "extra": json.dumps(
            {
                "account": "my_account",
                "database": "my_database",
                "warehouse": "my_warehouse",
                "private_key_content": "my_content",
            }
        ),
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = SnowflakeEncryptedPrivateKeyContentPemProfileMapping(
                conn,
            )
            assert not profile_mapping.can_claim_connection()

    # test when we're missing the account
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = '{"database": "my_database", "warehouse": "my_warehouse", "private_key_content": "my_private_key"}'
    print("testing with", conn.extra)
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyContentPemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # test when we're missing the database
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = '{"account": "my_account", "warehouse": "my_warehouse", "private_key_content": "my_private_key"}'
    print("testing with", conn.extra)
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyContentPemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # test when we're missing the warehouse
    conn = Connection(**potential_values)  # type: ignore
    conn.extra = '{"account": "my_account", "database": "my_database", "private_key_content": "my_private_key"}'
    print("testing with", conn.extra)
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyContentPemProfileMapping(conn)
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyContentPemProfileMapping(conn)
        assert profile_mapping.can_claim_connection()


def test_profile_mapping_selected(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
    )
    assert isinstance(
        profile_mapping, SnowflakeEncryptedPrivateKeyContentPemProfileMapping
    )


def test_profile_args(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the profile values get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
    )

    mock_account = mock_snowflake_conn.extra_dejson.get("account")
    mock_region = mock_snowflake_conn.extra_dejson.get("region")

    assert profile_mapping.profile == {
        "type": mock_snowflake_conn.conn_type,
        "user": mock_snowflake_conn.login,
        "private_key": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY') }}",
        "private_key_passphrase": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') }}",
        "schema": mock_snowflake_conn.schema,
        "account": f"{mock_account}.{mock_region}",
        "database": mock_snowflake_conn.extra_dejson.get("database"),
        "warehouse": mock_snowflake_conn.extra_dejson.get("warehouse"),
    }


def test_profile_args_overrides(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that you can override the profile values.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
        profile_args={"database": "my_db_override"},
    )
    assert profile_mapping.profile_args == {
        "database": "my_db_override",
    }

    mock_account = mock_snowflake_conn.extra_dejson.get("account")
    mock_region = mock_snowflake_conn.extra_dejson.get("region")

    assert profile_mapping.profile == {
        "type": mock_snowflake_conn.conn_type,
        "user": mock_snowflake_conn.login,
        "private_key": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY') }}",
        "private_key_passphrase": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') }}",
        "schema": mock_snowflake_conn.schema,
        "account": f"{mock_account}.{mock_region}",
        "database": "my_db_override",
        "warehouse": mock_snowflake_conn.extra_dejson.get("warehouse"),
    }


def test_profile_env_vars(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the environment variables get set correctly.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
    )
    assert profile_mapping.env_vars == {
        "COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": mock_snowflake_conn.password,
        "COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY": mock_snowflake_conn.extra_dejson.get(
            "private_key_content"
        ),
    }


def test_old_snowflake_format() -> None:
    """
    Tests that the old format still works.
    """
    conn = Connection(
        conn_id="my_snowflake_connection",
        conn_type="snowflake",
        login="my_user",
        schema="my_schema",
        password="secret",
        extra=json.dumps(
            {
                "extra__snowflake__account": "my_account",
                "extra__snowflake__database": "my_database",
                "extra__snowflake__warehouse": "my_warehouse",
                "extra__snowflake__private_key_content": "my_content",
            }
        ),
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = SnowflakeEncryptedPrivateKeyContentPemProfileMapping(conn)
        assert profile_mapping.profile == {
            "type": conn.conn_type,
            "user": conn.login,
            "private_key": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY') }}",
            "private_key_passphrase": "{{ env_var('COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') }}",
            "schema": conn.schema,
            "account": conn.extra_dejson.get("account"),
            "database": conn.extra_dejson.get("database"),
            "warehouse": conn.extra_dejson.get("warehouse"),
        }


def test_tranform_private_key(
    mock_snowflake_conn: Connection,
) -> None:
    """
    Tests that the private_key is not transformed to b64encoded key when the provided key is not pkcs#8.
    """

    profile_mapping = get_automatic_profile_mapping(
        mock_snowflake_conn.conn_id,
    )
    assert profile_mapping.env_vars[
        "COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY"
    ] == mock_snowflake_conn.extra_dejson.get("private_key_content")


def test_tranform_private_key_p8(
    mock_p8_snowflake_conn: Connection,
) -> None:
    """
    Tests that the private_key is not transformed to b64encoded key when the provided key is not pkcs#8.
    """

    def untransform_private_key(private_key_b64: str, private_key_passphrase: str):
        """
        Untransforms a b64encoded private_key to a pkcs#8 private_key
        Raises a ValueError if the key cannot be untransformed.
        """
        private_key = serialization.load_der_private_key(
            base64.b64decode(private_key_b64),
            password=private_key_passphrase.encode(),
        )

        return private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.BestAvailableEncryption(
                private_key_passphrase.encode()
            ),
        ).decode("utf-8")

    profile_mapping = get_automatic_profile_mapping(
        mock_p8_snowflake_conn.conn_id,
    )
    # We do not assert anything here, only that untransform_private_key does not raise a ValueError
    untransform_private_key(
        profile_mapping.env_vars["COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY"],
        profile_mapping.env_vars["COSMOS_CONN_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
    )
