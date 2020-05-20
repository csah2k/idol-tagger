
# idol-tagger

RSS Feeds Crawler for Microfocus IDOL, with Pyhton Spacy NLP and Doccano GUI

- in development ...

References:

- https://spacy.io/api
- https://finnhub.io/docs/api
- https://pythonhosted.org/feedparser/
- https://github.com/doccano/doccano
- https://github.com/afparsons/doccano_api_client
- https://github.com/csah2k/doccano_api_client
- https://www.microfocus.com/documentation/idol/IDOL_12_5/

## Requirements

>sudo apt install git python3 python3-pip docker-compose

>pip3 install spacy dataset feedparser html2text retrying plac

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
