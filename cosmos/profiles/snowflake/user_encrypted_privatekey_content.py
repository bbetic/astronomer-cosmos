"Maps Airflow Snowflake connections to dbt profiles if they use a user/private key."
from __future__ import annotations

import base64
import json
from typing import TYPE_CHECKING, Any

from cryptography.hazmat.primitives import serialization

from ..base import BaseProfileMapping

if TYPE_CHECKING:
    from airflow.models import Connection


class SnowflakeEncryptedPrivateKeyContentPemProfileMapping(BaseProfileMapping):
    """
    Maps Airflow Snowflake connections to dbt profiles if they use a user/private key.
    https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup#key-pair-authentication
    https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
    """

    airflow_connection_type: str = "snowflake"
    dbt_profile_type: str = "snowflake"
    is_community: bool = True

    required_fields = [
        "account",
        "user",
        "database",
        "warehouse",
        "schema",
        "private_key",
        "private_key_passphrase",
    ]
    secret_fields = [
        "private_key",
        "private_key_passphrase",
    ]
    airflow_param_mapping = {
        "account": "extra.account",
        "user": "login",
        "database": "extra.database",
        "warehouse": "extra.warehouse",
        "schema": "schema",
        "role": "extra.role",
        "private_key": "extra.private_key_content",
        "private_key_passphrase": "password",
    }

    @property
    def conn(self) -> Connection:
        """
        Snowflake can be odd because the fields used to be stored with keys in the format
        'extra__snowflake__account', but now are stored as 'account'.

        This standardizes the keys to be 'account', 'database', etc.
        """
        conn = super().conn

        conn_dejson = conn.extra_dejson

        if conn_dejson.get("extra__snowflake__account"):
            conn_dejson = {
                key.replace("extra__snowflake__", ""): value
                for key, value in conn_dejson.items()
            }

        conn.extra = json.dumps(conn_dejson)

        return conn

    @property
    def profile(self) -> dict[str, Any | None]:
        "Gets profile."
        profile_vars = {
            **self.mapped_params,
            **self.profile_args,
            # private_key and private_key_passphrase should always get set as env var
            "private_key": self.get_env_var_format("private_key"),
            "private_key_passphrase": self.get_env_var_format("private_key_passphrase"),
        }

        # remove any null values
        return self.filter_null(profile_vars)

    def transform_account(self, account: str) -> str:
        "Transform the account to the format <account>.<region> if it's not already."
        region = self.conn.extra_dejson.get("region")
        if region and region not in account:
            account = f"{account}.{region}"

        return str(account)

    def transform_private_key(self, private_key: str) -> str:
        """
        Attempts to transform the PKCS8 PEM private_key to a Base64-encoded DER
        If the transformation fails, assume the key is already Base64-encoded and return it
        DBT expectes private_key to be in Base64-encoded DER format or plain-text PEM
        https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup#key-pair-authentication
        """

        encoded_password = (
            None if self.conn.password is None else self.conn.password.encode()
        )

        try:
            private_key = serialization.load_pem_private_key(
                private_key.encode(),
                password=encoded_password,
            )
        except ValueError:
            return private_key

        return base64.b64encode(
            private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.BestAvailableEncryption(
                    encoded_password
                ),
            )
        ).decode("utf-8")
