import unittest
from unittest.mock import patch

from tap_spreadsheets_anywhere.format_handler import get_imap_fs

HOST = "outlook.office365.com"
TOKEN = {"access_token": "new-access-token", "refresh_token": "new-refresh-token"}


class TestGetImapFs(unittest.TestCase):
    def setUp(self):
        get_imap_fs.cache_clear()

    def test_refresh_token_flow(self):
        credentials = {
            "username": "test-user",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "tenant_id": "test-tenant-id",
            "refresh_token": "test-refresh-token",
        }

        with (
            patch(
                "tap_spreadsheets_anywhere.format_handler.get_transport_params",
                return_value=credentials,
            ),
            patch(
                "tap_spreadsheets_anywhere.format_handler.refresh_microsoft_token",
                return_value=TOKEN,
            ),
            patch("tap_spreadsheets_anywhere.format_handler.IMAPFileSystem") as fs,
        ):
            get_imap_fs(HOST)

        # the IMAP client takes the access token, and not the full token
        assert fs.call_args.kwargs == {
            "host": HOST,
            "username": "test-user",
            "access_token": "new-access-token",
        }

    def test_password(self):
        credentials = {"username": "test-user", "password": "test-password"}

        with (
            patch(
                "tap_spreadsheets_anywhere.format_handler.get_transport_params",
                return_value=credentials,
            ),
            patch(
                "tap_spreadsheets_anywhere.format_handler.refresh_microsoft_token",
            ) as refresh,
            patch("tap_spreadsheets_anywhere.format_handler.IMAPFileSystem") as fs,
        ):
            get_imap_fs(HOST)

        refresh.assert_not_called()
        assert fs.call_args.kwargs == {
            "host": HOST,
            "username": "test-user",
            "password": "test-password",
        }
