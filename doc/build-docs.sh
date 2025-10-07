#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
PLAYBOOK="local-antora-playbook.yml"

echo -e "${BLUE}Apache Cassandra Documentation Builder${NC}"
echo -e "${BLUE}=======================================${NC}\n"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${YELLOW}Error: Docker is not installed or not in PATH${NC}"
    echo "Please install Docker to build the documentation"
    exit 1
fi

# Clean previous build
if [ -d "$BUILD_DIR" ]; then
    echo -e "${YELLOW}Cleaning previous build...${NC}"
    rm -rf "$BUILD_DIR"
fi

# Run Antora in Docker
echo -e "${BLUE}Building documentation with Antora...${NC}"
docker run --rm \
    -v "$PROJECT_ROOT":/antora \
    -w /antora/doc \
    antora/antora:latest \
    "$PLAYBOOK"

# Check if build was successful
if [ -d "$BUILD_DIR/site" ]; then
    echo -e "\n${GREEN}✓ Documentation built successfully!${NC}\n"
    echo -e "${BLUE}Output location:${NC} $BUILD_DIR/site"
    echo -e "${BLUE}Main page:${NC} $BUILD_DIR/site/Cassandra/trunk/cassandra/index.html"
    echo -e "${BLUE}Compression docs:${NC} $BUILD_DIR/site/Cassandra/trunk/cassandra/managing/operating/compression.html"
    echo -e "\n${YELLOW}To view the documentation:${NC}"
    echo -e "  open $BUILD_DIR/site/Cassandra/trunk/cassandra/index.html"
    echo -e "\n${YELLOW}Or start a local web server:${NC}"
    echo -e "  cd $BUILD_DIR/site && python3 -m http.server 8080"
    echo -e "  Then open: ${BLUE}http://localhost:8080/Cassandra/trunk/cassandra/index.html${NC}"
else
    echo -e "\n${YELLOW}Warning: Build directory not found. Build may have failed.${NC}"
    exit 1
fi
