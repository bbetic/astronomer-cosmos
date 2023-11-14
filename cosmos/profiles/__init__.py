"Contains a function to get the profile mapping based on the connection ID."

from __future__ import annotations

from typing import Any, Type


from cosmos.profiles.athena import AthenaAccessKeyProfileMapping
from cosmos.profiles.base import BaseProfileMapping
from cosmos.profiles.bigquery.service_account_file import GoogleCloudServiceAccountFileProfileMapping
from cosmos.profiles.bigquery.service_account_keyfile_dict import GoogleCloudServiceAccountDictProfileMapping
from cosmos.profiles.bigquery.oauth import GoogleCloudOauthProfileMapping
from cosmos.profiles.databricks.token import DatabricksTokenProfileMapping
from cosmos.profiles.exasol.user_pass import ExasolUserPasswordProfileMapping
from cosmos.profiles.postgres.user_pass import PostgresUserPasswordProfileMapping
from cosmos.profiles.redshift.user_pass import RedshiftUserPasswordProfileMapping
from cosmos.profiles.snowflake.user_pass import SnowflakeUserPasswordProfileMapping
from cosmos.profiles.snowflake.user_privatekey import SnowflakePrivateKeyPemProfileMapping
from cosmos.profiles.snowflake.user_encrypted_privatekey import SnowflakeEncryptedPrivateKeyPemProfileMapping
from cosmos.profiles.snowflake.user_encrypted_privatekey_content import SnowflakeEncryptedPrivateKeyContentPemProfileMapping
from cosmos.profiles.spark.thrift import SparkThriftProfileMapping
from cosmos.profiles.trino.certificate import TrinoCertificateProfileMapping
from cosmos.profiles.trino.jwt import TrinoJWTProfileMapping
from cosmos.profiles.trino.ldap import TrinoLDAPProfileMapping
from cosmos.profiles.vertica.user_pass import VerticaUserPasswordProfileMapping

profile_mappings: list[Type[BaseProfileMapping]] = [
    AthenaAccessKeyProfileMapping,
    GoogleCloudServiceAccountFileProfileMapping,
    GoogleCloudServiceAccountDictProfileMapping,
    GoogleCloudOauthProfileMapping,
    DatabricksTokenProfileMapping,
    PostgresUserPasswordProfileMapping,
    RedshiftUserPasswordProfileMapping,
    SnowflakeEncryptedPrivateKeyContentPemProfileMapping,
    SnowflakeUserPasswordProfileMapping,
    SnowflakeEncryptedPrivateKeyPemProfileMapping,
    SnowflakePrivateKeyPemProfileMapping,
    SparkThriftProfileMapping,
    ExasolUserPasswordProfileMapping,
    TrinoLDAPProfileMapping,
    TrinoCertificateProfileMapping,
    TrinoJWTProfileMapping,
    VerticaUserPasswordProfileMapping,
]


def get_automatic_profile_mapping(
    conn_id: str,
    profile_args: dict[str, Any] | None = None,
) -> BaseProfileMapping:
    """
    Returns a profile mapping object based on the connection ID.
    """
    if not profile_args:
        profile_args = {}

    for profile_mapping in profile_mappings:
        mapping = profile_mapping(conn_id, profile_args)
        if mapping.can_claim_connection():
            return mapping

    raise ValueError(f"Could not find a profile mapping for connection {conn_id}.")


__all__ = [
    "BaseProfileMapping",
    "GoogleCloudServiceAccountFileProfileMapping",
    "GoogleCloudServiceAccountDictProfileMapping",
    "GoogleCloudOauthProfileMapping",
    "DatabricksTokenProfileMapping",
    "PostgresUserPasswordProfileMapping",
    "RedshiftUserPasswordProfileMapping",
    "SnowflakePrivateKeyContentPemProfileMapping",
    "SnowflakeUserPasswordProfileMapping",
    "SnowflakePrivateKeyPemProfileMapping",
    "SparkThriftProfileMapping",
    "ExasolUserPasswordProfileMapping",
    "TrinoLDAPProfileMapping",
    "TrinoCertificateProfileMapping",
    "TrinoJWTProfileMapping",
    "VerticaUserPasswordProfileMapping",
]
