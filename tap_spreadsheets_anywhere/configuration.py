"""Provides an object model for a our config file"""

import json
import logging

from voluptuous import Any, Extra, Optional, Required, Schema

LOGGER = logging.getLogger(__name__)

CONFIG_CONTRACT = Schema(
    {
        Required("tables"): [
            {
                Required("path"): str,
                Required("name"): str,
                Required("pattern"): str,
                Required("start_date"): str,
                Required("key_properties"): [str],
                Required("format"): Any("csv", "excel", "json", "jsonl", "detect"),
                Optional("encoding"): str,
                Optional("invalid_format_action"): Any("ignore", "fail"),
                Optional("universal_newlines"): bool,
                Optional("skip_initial"): int,
                Optional("selected"): bool,
                Optional("field_names"): [str],
                Optional("search_prefix"): str,
                Optional("worksheet_name"): str,
                Optional("delimiter"): str,
                Optional("quotechar"): str,
                Optional("json_path"): str,
                Optional("sample_rate"): int,
                Optional("max_sampling_read"): int,
                Optional("max_records_per_run"): int,
                Optional("max_sampled_files"): int,
                Optional("prefer_number_vs_integer"): bool,
                Optional("prefer_schema_as_string"): bool,
                Optional("schema_overrides"): {
                    str: {
                        Required("type"): Any(
                            Any(
                                "null",
                                "string",
                                "integer",
                                "number",
                                "date-time",
                                "object",
                            ),
                            [
                                Any(
                                    "null",
                                    "string",
                                    "integer",
                                    "number",
                                    "date-time",
                                    "object",
                                )
                            ],
                        )
                    }
                },
                Optional("ignore_undefined_field_names"): bool,
                Optional("ignore_state"): bool,
                Optional("skip_empty_rows"): bool,
                Optional("state_based_discovery"): bool,
            }
        ],
        Optional("azure_storage_connection_string"): str,
        Optional("aws_access_key_id"): str,
        Optional("aws_secret_access_key"): str,
        Optional("google_application_credentials"): str,
        Optional("ssh_private_key"): str,
        Optional("ssh_passphrase"): str,
        Optional("username"): str,
        Optional("password"): str,
        Optional("oauth_credentials"): {
            Optional("access_token"): str,
            Optional("refresh_token"): str,
            Optional("refresh_proxy_url"): str,
            Optional("refresh_proxy_url_auth"): str,
            Optional("client_id"): str,
            Optional("client_secret"): str,
            Optional("tenant_id"): str,
            Extra: object,
        },
        Optional("batch_config"): {
            Optional("encoding"): {
                Optional("format"): Any("arrow"),
            },
            Optional("storage"): {
                Optional("root"): str,
            },
            Optional("batch_size"): int,
        },
    }
)


class Config:
    @classmethod
    def dump(cls, config_json, ostream):
        json.dump(config_json, ostream, indent=2)

    @classmethod
    def validate(cls, config_json):
        CONFIG_CONTRACT(config_json)
        return config_json

    @classmethod
    def load(cls, filename):
        with open(filename) as fp:  # pylint: disable=invalid-name
            return Config.validate(json.load(fp))
