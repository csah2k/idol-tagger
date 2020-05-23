#!/bin/bash
# https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started-install.html

### DOCKER ###
# sudo docker pull docker.elastic.co/elasticsearch/elasticsearch:7.7.0

### CLASSIC ###
curl -L -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.7.0-linux-x86_64.tar.gz
tar -xvf elasticsearch-7.7.0-linux-x86_64.tar.gz
cd elasticsearch-7.7.0
bin/elasticsearch-plugin install https://github.com/spinscale/elasticsearch-ingest-langdetect/releases/download/7.7.0.1/ingest-langdetect-7.7.0.1.zip

