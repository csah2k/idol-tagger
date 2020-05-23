#!/bin/bash
# https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started-install.html

### DOCKER ###
# sudo docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.7.0

### CLASSIC ###
cd elasticsearch-7.7.0/bin
./elasticsearch
