#!/bin/bash
echo "=============== Cloning doccano_api_client ==============="

# https://github.com/afparsons/doccano_api_client
#git clone https://github.com/afparsons/doccano_api_client.git
git clone https://github.com/csah2k/doccano_api_client.git
cd doccano_api_client
pip3 install ./

echo "=============== Cloning doccano ==============="
cd ..
# https://github.com/doccano/doccano
git clone https://github.com/doccano/doccano.git




