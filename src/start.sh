#!/bin/bash
# https://github.com/doccano/doccano
# sudo docker-compose -f docker-compose.dev.yml up
cd doccano
#( sudo qterminal --execute "docker-compose -f docker-compose.dev.yml up" & ) > /dev/null 2>&1
sudo docker-compose -f docker-compose.prod.yml up


#echo "wait..."
#sleep 20
#xdg-open http://127.0.0.1:3000/
# Go to http://127.0.0.1:3000/
# ADMIN_USERNAME: "admin"
# ADMIN_PASSWORD: "password"
