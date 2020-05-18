#!/bin/bash
echo "downloading git srcs..."

# https://github.com/afparsons/doccano_api_client
#git clone https://github.com/afparsons/doccano_api_client.git
git clone https://github.com/csah2k/doccano_api_client.git
cd doccano_api_client
pip3 install ./


cd ..
# https://github.com/doccano/doccano
git clone https://github.com/doccano/doccano.git




