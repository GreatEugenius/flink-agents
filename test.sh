#!/bin/bash

mvn spotless:apply
mvn clean package -Dmaven.test.skip=true
./tools/build.sh
pip uninstall flink-agents
pip install ./python/dist/flink_agents-0.2.dev0-py3-none-any.whl