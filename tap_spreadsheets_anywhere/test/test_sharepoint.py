import unittest
from unittest.mock import patch

from tap_spreadsheets_anywhere.format_handler import get_sharepoint_fs

TOKEN = {"access_token": "new-access-token"}
URI = "sharepoint://test-site/Documents/"


class TestGetSharepointFs(unittest.TestCase):
    def setUp(self):
        get_sharepoint_fs.cache_clear()

    def _get_fs(self, credentials):
        with (
            patch(
                "tap_spreadsheets_anywhere.format_handler.get_transport_params",
                return_value=credentials,
            ),
            patch("tap_spreadsheets_anywhere.format_handler.MSGDriveFS") as fs,
        ):
            get_sharepoint_fs(URI)

        return fs

    def test_client_credentials_flow(self):
        fs = self._get_fs(
            {
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "tenant_id": "test-tenant-id",
            }
        )

        assert fs.call_args.kwargs == {
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "tenant_id": "test-tenant-id",
            "url_path": URI,
        }

    def test_client_credentials_flow_without_tenant_id(self):
        with self.assertRaises(ValueError):
            self._get_fs(
                {
                    "client_id": "test-client-id",
                    "client_secret": "test-client-secret",
                }
            )

    def test_no_credentials(self):
        with self.assertRaises(ValueError) as ctx:
            self._get_fs({"client_id": "test-client-id"})

        # the error lists every supported set of settings
        message = str(ctx.exception)
        assert "`access_token`" in message
        assert "`client_id`, `client_secret`, `tenant_id`" in message
        assert "`refresh_token`, `client_id`, `client_secret`" in message
        assert "`refresh_proxy_url`" in message

    def test_refresh_token_flow(self):
        with patch(
            "tap_spreadsheets_anywhere.format_handler.refresh_microsoft_token",
            return_value=TOKEN,
        ) as refresh:
            fs = self._get_fs(
                {
                    "client_id": "test-client-id",
                    "client_secret": "test-client-secret",
                    "tenant_id": "test-tenant-id",
                    "refresh_token": "test-refresh-token",
                }
            )

        refresh.assert_called_once()
        assert fs.call_args.kwargs == {
            "oauth2_client_params": {"token": TOKEN},
            "url_path": URI,
        }
