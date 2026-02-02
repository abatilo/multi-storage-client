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


from multistorageclient.commands.cli.actions.explorer import ExplorerAction


def test_name_returns_explorer():
    """Test that action name is 'explorer'."""
    action = ExplorerAction()
    assert action.name() == "explorer"


def test_help_returns_description():
    """Test that help returns a description."""
    action = ExplorerAction()
    help_text = action.help()
    assert "explorer" in help_text.lower() or "web" in help_text.lower()
