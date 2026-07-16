################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################
from importlib_resources import files
from importlib_resources.abc import Traversable
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.util.java_utils import add_jars_to_context_class_loader

from flink_agents.api.version_compatibility import flink_version_manager


def add_flink_agents_jars(env: StreamExecutionEnvironment) -> None:
    """Add the common and version-specific Flink Agents JARs to a job."""
    for jar_url in _flink_agents_jar_urls():
        env.add_jars(jar_url)


def add_flink_agents_jars_to_context_class_loader() -> None:
    """Make Flink Agents Java APIs visible to the current PyFlink gateway."""
    add_jars_to_context_class_loader(_flink_agents_jar_urls())


def _flink_agents_jar_urls() -> list[str]:
    major_version = flink_version_manager.major_version
    if not major_version:
        msg = "Apache Flink is not installed."
        raise ModuleNotFoundError(msg)

    lib_base = files("flink_agents.lib")
    return _jar_urls(lib_base / "common", "Flink Agents common JAR not found.") + (
        _jar_urls(
            lib_base / f"flink-{major_version}",
            f"Flink Agents dist JAR for Flink {major_version} not found.",
        )
    )


def _jar_urls(directory: Traversable, missing_message: str) -> list[str]:
    if not directory.is_dir():
        raise FileNotFoundError(missing_message)
    jar_urls = sorted(
        f"file://{jar_file}"
        for jar_file in directory.iterdir()
        if jar_file.is_file() and str(jar_file).endswith(".jar")
    )
    if not jar_urls:
        raise FileNotFoundError(missing_message)
    return jar_urls
