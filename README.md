
# index-flow

- in development ...

References:

- https://spacy.io/api
- https://finnhub.io/docs/api
- https://pythonhosted.org/feedparser/
- https://github.com/doccano/doccano
- https://pypi.org/project/django-admin-client/
- https://api.mongodb.com/python/current/tutorial.html
- https://elasticsearch-py.readthedocs.io/en/master/
- https://www.elastic.co/guide/en/elastic-stack-get-started/7.7/get-started-elastic-stack.html

## Requirements

>sudo apt install git python3 python3-pip docker-compose gunicorn

>pip3 install gunicorn flask spacy feedparser html2text retrying plac elasticsearch pymongo django-admin-client

>python3 -m spacy download en_core_web_sm<br/>
python3 -m spacy download pt_core_news_sm<br/>
python3 -m spacy download xx_ent_wiki_sm

> cd src/<br/>
sh ./install.sh

## Config

> gedit config.json

## Run

> cd src/<br/>
sh ./start.sh

> python3 main.py

ports 3000, 8000, 8080, 9100, 9200

> sudo ufw allow from any to any port XXX proto tcp
