#!/bin/bash

# neo4j
#docker run -p 7474:7474 -p 7687:7687 -v $PWD/neo4j_data:/data -e NEO4J_AUTH=none --rm neo4j
#docker run -p 7474:7474 -p 7687:7687 -v $PWD/neo4j_data_capriza:/data -e NEO4J_AUTH=none --rm neo4j

docker run -p 7474:7474 -p 7687:7687 -v $PWD/neo4j_data:/data -e NEO4J_AUTH=neo4j/salto --rm --env=NEO4J_ACCEPT_LICENSE_AGREEMENT=yes neo4j:enterprise

# orientdb
#docker run --rm --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0
