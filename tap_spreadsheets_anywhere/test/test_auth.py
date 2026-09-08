import unittest
from unittest.mock import patch

from tap_spreadsheets_anywhere.auth import refresh_microsoft_token

TOKEN = {"access_token": "new-access-token"}


class TestRefreshMicrosoftToken(unittest.TestCase):
    def test_refresh_with_app_registration(self):
        credentials = {
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "tenant_id": "test-tenant-id",
            "refresh_token": "test-refresh-token",
        }

        with patch("tap_spreadsheets_anywhere.auth.requests.post") as post:
            post.return_value.json.return_value = TOKEN
            token = refresh_microsoft_token(credentials)

        (url,) = post.call_args.args
        assert url == ("https://login.microsoftonline.com/test-tenant-id/oauth2/v2.0/token")
        assert post.call_args.kwargs["data"] == {
            "grant_type": "refresh_token",
            "refresh_token": "test-refresh-token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        }
        assert token == TOKEN

    def test_refresh_without_tenant_id(self):
        credentials = {
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
        }

        with patch("tap_spreadsheets_anywhere.auth.requests.post") as post:
            post.return_value.json.return_value = TOKEN
            refresh_microsoft_token(credentials)

        (url,) = post.call_args.args
        assert url == "https://login.microsoftonline.com/common/oauth2/v2.0/token"

    def test_refresh_with_proxy(self):
        # the proxy takes precedence over the configured app registration
        credentials = {
            "refresh_proxy_url": "https://test-proxy/token",
            "refresh_proxy_url_auth": "Bearer test-proxy-token",
            "refresh_token": "test-refresh-token",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "tenant_id": "test-tenant-id",
        }

        with patch("tap_spreadsheets_anywhere.auth.requests.post") as post:
            post.return_value.json.return_value = TOKEN
            token = refresh_microsoft_token(credentials)

        (url,) = post.call_args.args
        assert url == "https://test-proxy/token"
        assert post.call_args.kwargs["headers"] == {
            "Authorization": "Bearer test-proxy-token",
        }
        assert post.call_args.kwargs["json"] == {
            "grant_type": "refresh_token",
            "refresh_token": "test-refresh-token",
        }
        assert token == TOKEN
