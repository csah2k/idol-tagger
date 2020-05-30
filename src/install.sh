#!/bin/bash
echo "=============== Cloning doccano ==============="
cd ..
# https://github.com/doccano/doccano
git clone https://github.com/doccano/doccano.git

echo "=============== Cloning doccano_api_client ==============="
# https://github.com/afparsons/doccano_api_client
#git clone https://github.com/afparsons/doccano_api_client.git
#git clone https://github.com/csah2k/doccano_api_client.git
#cd doccano_api_client
git clone -b create-stuff https://github.com/harmw/doccano-client.git
cd doccano-client
pip3 install ./

#echo "=============== installing django admin client ==============="
#git clone https://gitlab.com/y3g0r/django-admin-client.git
#cd django-admin-client
#pip3 install ./
# $ python3 
# >>> import django_admin_client.command_line.generate_package
# >>> django_admin_client.command_line.generate_package.main()
#cd doccano-admin-client
#pip3 install ./
