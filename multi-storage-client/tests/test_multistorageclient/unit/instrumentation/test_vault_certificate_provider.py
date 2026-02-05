# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.instrumentation.auth import CertificatePaths, VaultCertificateProvider


@pytest.fixture
def vault_config():
    """Fixture providing basic Vault configuration."""
    return {
        "vault_endpoint": "https://vault.example.com",
        "vault_namespace": "test-namespace",
        "approle_id": "test-role-id",
        "approle_secret": "test-secret-id",
    }


@pytest.fixture
def mock_hvac():
    """Fixture to mock hvac library."""
    with patch("multistorageclient.instrumentation.auth.hvac") as mock:
        yield mock


@pytest.fixture
def provider_with_mocked_vault(vault_config, mock_hvac):
    """Fixture to create a VaultCertificateProvider with mocked Vault client."""
    mock_client = MagicMock()
    mock_hvac.Client.return_value = mock_client

    mock_client.auth.approle.login.return_value = {"auth": {"client_token": "test-client-token"}}

    mock_client.secrets.kv.v2.read_secret_version.return_value = {
        "data": {
            "data": {
                "cert": "-----BEGIN CERTIFICATE-----\ntest-cert\n-----END CERTIFICATE-----",
                "key": "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----",
                "ca": "-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----",
            }
        }
    }

    provider = VaultCertificateProvider(**vault_config)
    yield provider, mock_client, mock_hvac


class TestVaultCertificateProvider:
    def test_init_with_defaults(self, vault_config):
        """Test initialization with default values."""
        provider = VaultCertificateProvider(**vault_config)

        assert provider._vault_endpoint == vault_config["vault_endpoint"]
        assert provider._vault_namespace == vault_config["vault_namespace"]
        assert provider._approle_id == vault_config["approle_id"]
        assert provider._approle_secret == vault_config["approle_secret"]
        assert provider._secret_path == "certificates"
        assert provider._mount_point == "secret"
        assert provider._cert_key == "cert"
        assert provider._key_key == "key"
        assert provider._ca_key == "ca"

    def test_init_with_custom_values(self, vault_config):
        """Test initialization with custom values."""
        provider = VaultCertificateProvider(
            **vault_config,
            secret_path="custom/path",
            mount_point="custom/mount",
            cert_key="custom_cert",
            key_key="custom_key",
            ca_key="custom_ca",
        )

        assert provider._secret_path == "custom/path"
        assert provider._mount_point == "custom/mount"
        assert provider._cert_key == "custom_cert"
        assert provider._key_key == "custom_key"
        assert provider._ca_key == "custom_ca"

    def test_get_cert_cache_dir(self, vault_config):
        """Test certificate cache directory path generation."""
        provider = VaultCertificateProvider(**vault_config)
        cache_dir = provider._get_cert_cache_dir()
        expected_dir = os.path.join(tempfile.gettempdir(), "msc", "observability", "mtls")
        assert cache_dir == expected_dir

    def test_authenticate_to_vault_success(self, provider_with_mocked_vault):
        """Test successful authentication to Vault."""
        provider, mock_client, _ = provider_with_mocked_vault

        token = provider._authenticate_to_vault()

        assert token == "test-client-token"
        mock_client.auth.approle.login.assert_called_once_with(
            role_id="test-role-id",
            secret_id="test-secret-id",
        )

    def test_authenticate_to_vault_failure(self, vault_config, mock_hvac):
        """Test authentication failure handling."""
        mock_client = MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.auth.approle.login.side_effect = Exception("Auth failed")

        provider = VaultCertificateProvider(**vault_config)

        with patch("multistorageclient.instrumentation.auth.time.sleep"):
            with pytest.raises(RuntimeError, match="Failed to authenticate to Vault"):
                provider._authenticate_to_vault()

    def test_fetch_certificates_from_vault_success(self, provider_with_mocked_vault):
        """Test successful certificate fetching."""
        provider, mock_client, _ = provider_with_mocked_vault

        certs = provider._fetch_certificates_from_vault("test-token")

        assert "cert" in certs
        assert "key" in certs
        assert "ca" in certs
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="certificates",
            mount_point="secret",
            raise_on_deleted_version=True,
        )

    def test_fetch_certificates_missing_key(self, vault_config, mock_hvac):
        """Test handling of missing certificate key."""
        mock_client = MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {
                "data": {
                    "cert": "test-cert",
                    # Missing "key" and "ca"
                }
            }
        }

        provider = VaultCertificateProvider(**vault_config)

        with patch("multistorageclient.instrumentation.auth.time.sleep"):
            with pytest.raises(RuntimeError, match="Failed to fetch certificates"):
                provider._fetch_certificates_from_vault("test-token")

    def test_write_certificates_to_disk(self, vault_config):
        """Test writing certificates to disk with correct permissions."""
        provider = VaultCertificateProvider(**vault_config)

        cert_data = {
            "cert": "-----BEGIN CERTIFICATE-----\ntest-cert\n-----END CERTIFICATE-----",
            "key": "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----",
            "ca": "-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----",
        }

        try:
            paths = provider._write_certificates_to_disk(cert_data)

            assert isinstance(paths, CertificatePaths)
            assert os.path.exists(paths.client_certificate_file)
            assert os.path.exists(paths.client_key_file)
            assert os.path.exists(paths.certificate_file)

            # Verify file contents
            with open(paths.client_certificate_file) as f:
                assert f.read() == cert_data["cert"]
            with open(paths.client_key_file) as f:
                assert f.read() == cert_data["key"]
            with open(paths.certificate_file) as f:
                assert f.read() == cert_data["ca"]

            # Verify permissions
            cert_mode = os.stat(paths.client_certificate_file).st_mode & 0o777
            key_mode = os.stat(paths.client_key_file).st_mode & 0o777
            ca_mode = os.stat(paths.certificate_file).st_mode & 0o777

            assert cert_mode == 0o644
            assert key_mode == 0o600
            assert ca_mode == 0o644

        finally:
            # Cleanup
            import shutil

            cert_dir = provider._get_cert_cache_dir()
            if os.path.exists(cert_dir):
                shutil.rmtree(os.path.dirname(os.path.dirname(cert_dir)))

    def test_get_certificates_full_flow(self, provider_with_mocked_vault):
        """Test full certificate fetching flow."""
        provider, _, _ = provider_with_mocked_vault

        try:
            paths = provider.get_certificates()

            assert isinstance(paths, CertificatePaths)
            assert os.path.exists(paths.client_certificate_file)
            assert os.path.exists(paths.client_key_file)
            assert os.path.exists(paths.certificate_file)

        finally:
            # Cleanup
            import shutil

            cert_dir = provider._get_cert_cache_dir()
            if os.path.exists(cert_dir):
                shutil.rmtree(os.path.dirname(os.path.dirname(cert_dir)))

    def test_get_certificates_uses_in_memory_cache(self, provider_with_mocked_vault):
        """Test that in-memory cached certificates are returned on subsequent calls."""
        provider, mock_client, _ = provider_with_mocked_vault

        try:
            paths1 = provider.get_certificates()
            paths2 = provider.get_certificates()

            assert paths1 == paths2
            # Auth should only be called once
            assert mock_client.auth.approle.login.call_count == 1

        finally:
            # Cleanup
            import shutil

            cert_dir = provider._get_cert_cache_dir()
            if os.path.exists(cert_dir):
                shutil.rmtree(os.path.dirname(os.path.dirname(cert_dir)))

    def test_get_certificates_uses_disk_cache_cross_instance(self, vault_config, mock_hvac):
        """Test that a new provider instance uses certificates cached on disk by another instance."""
        mock_client = MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.auth.approle.login.return_value = {"auth": {"client_token": "test-client-token"}}
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {
                "data": {
                    "cert": "-----BEGIN CERTIFICATE-----\ntest-cert\n-----END CERTIFICATE-----",
                    "key": "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----",
                    "ca": "-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----",
                }
            }
        }

        try:
            # First provider fetches from Vault
            provider1 = VaultCertificateProvider(**vault_config)
            paths1 = provider1.get_certificates()
            assert mock_client.auth.approle.login.call_count == 1

            # Second provider (simulating another process) should use disk cache
            provider2 = VaultCertificateProvider(**vault_config)
            paths2 = provider2.get_certificates()

            # Vault should NOT be called again
            assert mock_client.auth.approle.login.call_count == 1
            assert paths1 == paths2

        finally:
            import shutil

            cert_dir = provider1._get_cert_cache_dir()
            if os.path.exists(cert_dir):
                shutil.rmtree(os.path.dirname(os.path.dirname(cert_dir)))


class TestCertificatePaths:
    def test_certificate_paths_dataclass(self):
        """Test CertificatePaths dataclass."""
        paths = CertificatePaths(
            client_certificate_file="/path/to/cert.crt",
            client_key_file="/path/to/key.key",
            certificate_file="/path/to/ca.crt",
        )

        assert paths.client_certificate_file == "/path/to/cert.crt"
        assert paths.client_key_file == "/path/to/key.key"
        assert paths.certificate_file == "/path/to/ca.crt"
