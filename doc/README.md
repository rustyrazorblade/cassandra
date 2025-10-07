<!--
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
-->

Apache Cassandra documentation directory
========================================

This directory contains the documentation maintained in-tree for Apache
Cassandra. This directory contains the following documents:
- The source of the official Cassandra documentation, in the `source/modules`
  subdirectory. See below for more details on how to edit/build that
  documentation.
- The specification(s) for the supported versions of native transport protocol.


Official documentation
----------------------

The source for the official documentation for Apache Cassandra can be found in
the `modules/cassandra/pages` subdirectory. The documentation uses [antora](http://www.antora.org/)
and is thus written in [asciidoc](http://asciidoc.org).

To generate the asciidoc files for cassandra.yaml and the nodetool commands, run (from project root):
```bash
ant gen-asciidoc
```
or (from this directory):

```bash
make gen-asciidoc
```


To build the documentation locally using Docker and Antora (recommended):

```bash
# From the doc/ directory
./build-docs.sh
```

This will:
- Build the documentation using the official Antora Docker image
- Output HTML files to `doc/build/site/`
- Provide instructions for viewing the documentation

To view the built documentation:

```bash
# Open directly in browser
open build/site/Cassandra/trunk/cassandra/index.html

# Or start a local web server
cd build/site
python3 -m http.server 8080
# Then navigate to: http://localhost:8080/Cassandra/trunk/cassandra/index.html
```

Alternatively, to build using a local Antora installation (requires Node.js):

```bash
# From project root
ant gen-doc

# Or from this directory
make html
```

For the full production documentation build, see the build instructions in the [cassandra-website](https://github.com/apache/cassandra-website) repo.

