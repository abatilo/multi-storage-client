# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

"""Pytest fixtures for MSC Explorer tests."""

import pytest

pytest.importorskip("fastapi", reason="FastAPI required for explorer tests")
pytest.importorskip("httpx", reason="httpx required for FastAPI testing")

from unittest.mock import MagicMock

# import importlib
# from typing import Any, cast
from fastapi.testclient import TestClient

# Import the server module directly
import multistorageclient.explorer.api.server as app_module


@pytest.fixture
def mock_msc_config():
    """Mock MSC configuration with test profiles."""
    return {
        "profiles": {
            "test-s3": {
                "storage_provider": {
                    "type": "s3",
                    "options": {
                        "bucket": "test-bucket",
                        "region": "us-west-2",
                    },
                },
            },
            "test-local": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": "/tmp/test",
                    },
                },
            },
        }
    }


@pytest.fixture
def mock_storage_client():
    """Create a mock StorageClient."""
    client = MagicMock()
    return client


@pytest.fixture
def test_client(mock_msc_config):
    """Create a FastAPI test client with mocked MSC."""
    # Set the global config
    app_module.msc_config = mock_msc_config

    with TestClient(app_module.app) as client:
        yield client

    # Clean up
    app_module.msc_config = None
    app_module._client_cache.clear()
