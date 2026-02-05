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

"""
Tests for vanilla OTLP exporter with mTLS certificates.

This tests the standard OTLPMetricExporter when users provide certificate
file paths directly in the config (without using a certificate provider).

Example config:
    opentelemetry:
      metrics:
        exporter:
          type: otlp
          options:
            endpoint: "https://otlp.example.com/v1/metrics"
            client_certificate_file: /path/to/cert.crt
            client_key_file: /path/to/key.key
            certificate_file: /path/to/ca.crt
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.telemetry import _METRICS_EXPORTER_MAPPING


@pytest.fixture
def temp_cert_files():
    """Create temporary certificate files for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_path = os.path.join(tmpdir, "cert.crt")
        key_path = os.path.join(tmpdir, "key.key")
        ca_path = os.path.join(tmpdir, "ca.crt")

        for path, content in [
            (cert_path, "-----BEGIN CERTIFICATE-----\ntest-cert\n-----END CERTIFICATE-----"),
            (key_path, "-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----"),
            (ca_path, "-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----"),
        ]:
            with open(path, "w") as f:
                f.write(content)

        yield {
            "cert_path": cert_path,
            "key_path": key_path,
            "ca_path": ca_path,
            "tmpdir": tmpdir,
        }


class TestOTLPExporterMapping:
    def test_otlp_exporter_mapping_exists(self):
        """Test that otlp exporter is in the metrics exporter mapping."""
        assert "otlp" in _METRICS_EXPORTER_MAPPING
        assert (
            _METRICS_EXPORTER_MAPPING["otlp"]
            == "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter"
        )


class TestOTLPExporterWithMTLS:
    def test_exporter_accepts_mtls_options(self, temp_cert_files):
        """Test that OTLPMetricExporter accepts mTLS certificate options."""
        with patch(
            "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter.__init__",
            return_value=None,
        ) as mock_init:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

            exporter_options = {
                "endpoint": "https://otlp.example.com/v1/metrics",
                "client_certificate_file": temp_cert_files["cert_path"],
                "client_key_file": temp_cert_files["key_path"],
                "certificate_file": temp_cert_files["ca_path"],
            }

            OTLPMetricExporter(**exporter_options)

            mock_init.assert_called_once_with(**exporter_options)

    def test_exporter_with_additional_options(self, temp_cert_files):
        """Test that additional OTLP options are passed through alongside mTLS options."""
        with patch(
            "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter.__init__",
            return_value=None,
        ) as mock_init:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

            exporter_options = {
                "endpoint": "https://otlp.example.com/v1/metrics",
                "client_certificate_file": temp_cert_files["cert_path"],
                "client_key_file": temp_cert_files["key_path"],
                "certificate_file": temp_cert_files["ca_path"],
                "timeout": 30,
                "compression": "gzip",
            }

            OTLPMetricExporter(**exporter_options)

            call_kwargs = mock_init.call_args[1]
            assert call_kwargs["endpoint"] == "https://otlp.example.com/v1/metrics"
            assert call_kwargs["client_certificate_file"] == temp_cert_files["cert_path"]
            assert call_kwargs["client_key_file"] == temp_cert_files["key_path"]
            assert call_kwargs["certificate_file"] == temp_cert_files["ca_path"]
            assert call_kwargs["timeout"] == 30
            assert call_kwargs["compression"] == "gzip"


class TestTelemetryMeterProviderWithMTLS:
    def test_meter_provider_creates_exporter_with_mtls(self, temp_cert_files):
        """Test that Telemetry.meter_provider correctly creates OTLP exporter with mTLS options."""
        from multistorageclient.telemetry import Telemetry

        config = {
            "exporter": {
                "type": "otlp",
                "options": {
                    "endpoint": "https://otlp.example.com/v1/metrics",
                    "client_certificate_file": temp_cert_files["cert_path"],
                    "client_key_file": temp_cert_files["key_path"],
                    "certificate_file": temp_cert_files["ca_path"],
                },
            },
        }

        with patch("opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter") as mock_exporter_class:
            mock_exporter = MagicMock()
            mock_exporter_class.return_value = mock_exporter

            with patch(
                "multistorageclient.telemetry.metrics.readers.diperiodic_exporting.DiperiodicExportingMetricReader"
            ):
                telemetry = Telemetry()
                telemetry.meter_provider(config)

                mock_exporter_class.assert_called_once()
                call_kwargs = mock_exporter_class.call_args[1]

                assert call_kwargs["endpoint"] == "https://otlp.example.com/v1/metrics"
                assert call_kwargs["client_certificate_file"] == temp_cert_files["cert_path"]
                assert call_kwargs["client_key_file"] == temp_cert_files["key_path"]
                assert call_kwargs["certificate_file"] == temp_cert_files["ca_path"]
