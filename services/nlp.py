from __future__ import unicode_literals, print_function
import os
import re 
import csv
import json
import plac
import random
import codecs
import logging
import dataset
import datetime
import concurrent.futures
from retrying import retry
from pathlib import Path
import thinc.extra.datasets
import spacy
from spacy.util import minibatch, compounding
from doccano_api_client import DoccanoClient
from requests.structures import CaseInsensitiveDict
filters_fieldprefix = 'FILTERINDEX'


# https://github.com/doccano/doccano
# https://github.com/afparsons/doccano_api_client

TRAIN_DATA = []


class Service:

    executor = None
    logging = None
    config = None
    idol = None
    db = None
    doccano_client = None

    def __init__(self, logging, config, idol): 
        self.logging = logging 
        self.config = config.get('nlp').copy()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 2), thread_name_prefix='NlpPool')
        self.idol = idol 
        login = self.config.get('doccano')
        self.doccano_login(login.get('url'), login.get('username'), login.get('password'))
        self.db = dataset.connect('sqlite:///:memory:') # change to file persistence


    def doccano_login(self, url, username, password):
        return self.executor.submit(self._doccano_login, url, username, password).result()

    def _doccano_login(self, url, username, password):
        self.doccano_client = DoccanoClient(url, username, password)
        r_me = self.doccano_client.get_me().json()
        self.logging.info(f"Doccano login: {r_me}")
        if not r_me.get('is_superuser'):
            self.logging.warn(f"User username is not a super-user!")
        return self.doccano_client
    
    def saveDB(self):
        table = self.db['sometable']
        table.insert(dict(name='John Doe', age=37))
        table.insert(dict(name='Jane Doe', age=34, gender='female'))
        _john = table.find_one(name='John Doe')

    def populateProject(self, project):
        project_name = project.get('name').strip()
        projects_list = self.doccano_client.get_project_list().json()
        projects = [_p for _p in projects_list if _p.get('name') == project_name]
        self.logging.debug(f"all projects: {projects}")
        if len(projects) == 0:
            self.logging.error(f"Project name '{project_name}' not exists!")
            return project

        project['id'] = projects[0].get('id')
        project['project_type'] = projects[0].get('project_type')
        self.logging.info(f"Docano project: '{project_name}', id: {project.get('id')}")
        return project

    def export_idol_to_doccano(self, project):
        self.logging.info(f"==== Exporting ====>  Idol -> Doccano ({project.get('name')})")
        self.export_training_from_idol(project)
        self.import_training_into_doccano(project)

    def export_training_from_idol(self, project):
        return self.executor.submit(self._export_training_from_idol, project).result()

    def _export_training_from_idol(self, project):
        tempFile = project.get('tempfile', project.get('name')+'.tmp')
        dataFolder = self.config.get('tempfolder', 'data')
        target_file = os.path.join(dataFolder, tempFile)
        if os.path.exists(target_file): os.remove(target_file)
        with codecs.open(target_file, 'a', 'utf-8') as outfile:
            for _query in project.get('queries'):
                query = _query.copy()
                # filter documents already in this training project
                #fieldText = query.get('fieldtext','')
                #fieldTextFilter = f"NOT EXISTS{{}}:{project.get('datafield')}"
                #if len(fieldText) > 1: fieldText = f"({fieldText}) AND ({fieldTextFilter})"
                #else: fieldText = fieldTextFilter
                #query['fieldtext'] = fieldText  
                


                query['print'] = 'all'
                docsToMove = []
                hits = self.idol.query(query)
                self.logging.info(f"IDOL - hits: {len(hits)}, query: {query}")
                for hit in hits:
                    doc = hit.get('content',{}).get('DOCUMENT',[{}])[0]
                    text = doc.get(project.get('textfield'), [''])[0]
                    if len(text.strip()) > 10:
                        # CLASSIFICATION  ====>   {"text": "Great price.", "labels": ["positive"]}
                        # ENTITY EXTRACTION ==>   {"text": "President Obama", "labels": [ [10, 15, "PERSON"] ]}
                        labels = json.loads(doc.get(project.get('datafield'), ['[]'])[0])
                        hit['fields'] = [(project.get('datafield'), labels)]
                        meta = {
                            'link': getDocLink(doc),
                            'date': getDocDate(doc),
                            'language': doc.get('LANGUAGE', ''),
                            'reference': hit.get('reference')
                        }
                        meta.update(getDocFilters(doc))
                        jsonl = json.dumps({'text': text, 'labels': labels, 'meta': meta}, ensure_ascii=False).encode('utf8')
                        outfile.write(jsonl.decode()+'\n')
                        docsToMove.append(hit)
                # MOVE the selected docs to a staging database
                if len(docsToMove) > 0: 
                    query = {
                        'DREDbName': project.get('database'),
                        'KillDuplicates': 'REFERENCE', ## check for the same reference ONLY in 'DREDbName' database
                        'CreateDatabase': True,
                        'KeepExisting': True, ## do not replace content for matched references in KillDuplicates
                        'Priority': 100
                    }
                    self.idol.index_into_idol(docsToMove, query)
                
    def import_training_into_doccano(self, project):
        return self.executor.submit(self._import_training_into_doccano, project).result()

    def _import_training_into_doccano(self, project):
        project = self.populateProject(project)
        tempFile = project.get('tempfile', project.get('name')+'.tmp')
        dataFolder = self.config.get('tempfolder', 'data')
        file_path = os.path.join(dataFolder, tempFile)
        if not os.path.exists(file_path):
            self.logging.error(f"File not exists: {file_path}")
            return
        resp = self.doccano_client.post_doc_upload(project.get('id'), 'json', tempFile, dataFolder)
        if 200 <= resp.status_code < 300:
            self.logging.info(f"File uploaded to Doccano ({resp.status_code}): '{file_path}'")
            if os.path.exists(file_path): os.remove(file_path)
        else:
            self.logging.error(f"Erro uploading file to Doccano: {tempFile}, code: {resp.status_code}")
        return resp

    def export_doccano_to_idol(self, project):
        self.logging.info(f"==== Exporting ====>  Doccano -> Idol ({project.get('name')})")
        return self.executor.submit(self._export_doccano_to_idol, project).result()

    def _export_doccano_to_idol(self, project):
        project = self.populateProject(project)
        resp = self.doccano_client.get_doc_download(project.get('id'), 'json')
        docsToIndex = []
        for line in resp.text.splitlines():
            self.logging.debug(line)
            doccanoDoc = json.loads(line)
            if doccanoDoc.get('annotation_approver', None) != None:            
                reference = doccanoDoc.get('meta',{}).get('reference')
                labels = json.dumps(doccanoDoc.get('annotations',[]), ensure_ascii=False).encode('utf8')
                text = doccanoDoc.get('text','')
                # get same doc from idol
                query = {
                    'DatabaseMatch': project.get('database'),
                    'Reference': reference
                }
                idolDoc = self.idol.get_content(query)
                self.logging.debug(idolDoc)
                if idolDoc != None:
                    self.logging.info(f"Document exported [doccano:{doccanoDoc.get('id')} -> idol:{idolDoc.get('id')}]") 
                    # update fields
                    idolDoc['content']['DOCUMENT'][0][project.get('textfield')] = [text]
                    idolDoc['content']['DOCUMENT'][0][project.get('datafield')] = [labels.decode()]                
                    docsToIndex.append(idolDoc)
                    self.doccano_client.delete_document(project.get('id'), doccanoDoc.get('id'))
                else:
                    self.logging.warn(f"Can't find reference in idol [doccano:{doccanoDoc.get('id')} -> idol:{reference}]")
        if len(docsToIndex) > 0: 
            # index updated document
            query = {
                'DREDbName': project.get('database'),
                'KillDuplicates': 'REFERENCE',
                'CreateDatabase': True,
                'Priority': 100
            }
            self.idol.index_into_idol(docsToIndex, query)

    def load_classifier_data(self, project, limit=100, split=0.8):
        project = self.populateProject(project)
        train_data = []
        # query documents from this project database in IDOL
        query = {
            'DatabaseMatch': project.get('database'),
            'FieldText': f"TERM{{label}}:{project.get('datafield')}",
            'PrintFields': f"{project.get('datafield')},{project.get('textfield')}",
            'AnyLanguage': True, ## TODO run this 'load_classifier_data' and 'train_model_classifier' for each existing LANGUAGE values
            'MaxResults': limit,
            'Text': '*'
        }
        hits = self.idol.query(query)

        # parse documents into expected 'spacy' format
        for hit in hits:
            text = hit['content']['DOCUMENT'][0][project.get('textfield')][0]
            data = json.loads(hit['content']['DOCUMENT'][0][project.get('datafield')][0])
            labels = [_l.get('label') for _l in data]
            train_data.append((text, labels)) 
        if len(train_data) > 1:
            random.shuffle(train_data)
            train_data = train_data[-limit:]
            texts, labels = zip(*train_data)

            # list categories (labels) from Doccano
            project_labels = self.doccano_client.get_label_list(project.get('id'))
            labels_map = {}
            labels_template = {}
            for _l in project_labels.json():
                labels_map[_l.get('id')] = _l.get('text')
                labels_template[_l.get('text')] = False

            # extract categories
            cats = []
            for _lbls in labels:
                cat = labels_template.copy()
                for _l in _lbls:
                    cat.update({labels_map[_l]:True})
                cats.append(cat)

            # Partition off part of the train data for evaluation
            split = int(len(train_data) * split)
            return (texts[:split], cats[:split]), (texts[split:], cats[split:]), list(labels_template.keys())
        self.logging.warn(f"No enough Idol results to create training data: {len(train_data)}")
        return ([], []), ([], []), []

    def train_model_classifier(self, project, model=None, output_dir=None, n_iter=20, n_texts=2000, init_tok2vec=None):  
        return self.executor.submit(self._train_model_classifier, project, model, output_dir, n_iter, n_texts, init_tok2vec).result()

    def _train_model_classifier(self, project, model=None, output_dir=None, n_iter=20, n_texts=2000, init_tok2vec=None):  
        self.logging.info(f"==== Training model ====>  ({project.get('name')})")  
        if model is not None:
            self.logging.info("Loading model '%s'" % model)
            nlp = spacy.load(model)  # load existing spaCy model
        else:
            self.logging.info("Creating blank 'en' model")
            nlp = spacy.blank("en")  # create blank Language class
            
        # add the text classifier to the pipeline if it doesn't exist
        # nlp.create_pipe works for built-ins that are registered with spaCy
        if "textcat" not in nlp.pipe_names:
            textcat = nlp.create_pipe("textcat", config={"exclusive_classes": True, "architecture": "simple_cnn"})
            nlp.add_pipe(textcat, last=True)
        # otherwise, get it, so we can add labels to it
        else:
            textcat = nlp.get_pipe("textcat")

        # load correct dataset from IDOL
        self.logging.info("Loading classifier data...")
        (train_texts, train_cats), (dev_texts, dev_cats), categories = self.load_classifier_data(project)
        if len(categories) == 0 or len(train_texts) == 0:
            self.logging.info("No new training data found in idol")
            return

        # add label to text classifier
        self.logging.info(f"Categories: {categories}")
        for cat in categories:
            textcat.add_label(cat)

        train_texts = train_texts[:n_texts]
        train_cats = train_cats[:n_texts]
        self.logging.info(f"Using {n_texts} examples ({len(train_texts)} training, {len(dev_texts)} evaluation)")
        train_data = list(zip(train_texts, [{"cats": cats} for cats in train_cats]))

        # get names of other pipes to disable them during training
        pipe_exceptions = ["textcat", "trf_wordpiecer", "trf_tok2vec"]
        other_pipes = [pipe for pipe in nlp.pipe_names if pipe not in pipe_exceptions]
        with nlp.disable_pipes(*other_pipes):  # only train textcat
            optimizer = nlp.begin_training()
            if init_tok2vec is not None:
                with init_tok2vec.open("rb") as file_:
                    textcat.model.tok2vec.from_bytes(file_.read())
            self.logging.info("Training the model...")
            self.logging.debug("{:^5}\t{:^5}\t{:^5}\t{:^5}".format("LOSS", "P", "R", "F"))
            batch_sizes = compounding(4.0, 32.0, 1.001)
            for _i in range(n_iter):
                losses = {}
                # batch up the examples using spaCy's minibatch
                random.shuffle(train_data)
                batches = minibatch(train_data, size=batch_sizes)
                for batch in batches:
                    texts, annotations = zip(*batch)
                    nlp.update(texts, annotations, sgd=optimizer, drop=0.2, losses=losses)
                with textcat.model.use_params(optimizer.averages):
                    # evaluate on the dev data split off in load_data()
                    scores = self.evaluate(nlp.tokenizer, textcat, dev_texts, dev_cats)
                self.logging.debug(
                    "{0:.3f}\t{1:.3f}\t{2:.3f}\t{3:.3f}".format(  # print a simple table
                        losses["textcat"],
                        scores["textcat_p"],
                        scores["textcat_r"],
                        scores["textcat_f"],
                    )
                )
        # test the trained model
        test_text = "Aggressive treatment against covid war in all countries"
        doc = nlp(test_text)
        best_cat = ('', 0)
        for _cat in doc.cats:
            if doc.cats[_cat] > best_cat[1]:
                best_cat = (_cat, doc.cats[_cat])
        self.logging.info(f"{best_cat} : {test_text}")

        if output_dir is not None:
            with nlp.use_params(optimizer.averages):
                nlp.to_disk(output_dir)
            self.logging.info(f"Saved model to {output_dir}")

            # test the saved model
            self.logging.info(f"Loading from {output_dir}")
            nlp2 = spacy.load(output_dir)
            doc2 = nlp2(test_text)
            self.logging.info(test_text, doc2.cats)
                
 
    @plac.annotations(
        model=("Model name. Defaults to blank 'en' model.", "option", "m", str),
        output_dir=("Optional output directory", "option", "o", Path),
        n_iter=("Number of training iterations", "option", "n", int),
    )
    def train_model_ner(self, model=None, output_dir=None, n_iter=100):
        """Load the model, set up the pipeline and train the entity recognizer."""
        if model is not None:
            nlp = spacy.load(model)  # load existing spaCy model
            logging.info("Loaded model '%s'" % model)
        else:
            nlp = spacy.blank("en")  # create blank Language class
            logging.info("Created blank 'en' model")

        # create the built-in pipeline components and add them to the pipeline
        # nlp.create_pipe works for built-ins that are registered with spaCy
        if "ner" not in nlp.pipe_names:
            ner = nlp.create_pipe("ner")
            nlp.add_pipe(ner, last=True)
        # otherwise, get it so we can add labels
        else:
            ner = nlp.get_pipe("ner")

        # add labels
        for _, annotations in TRAIN_DATA:
            for ent in annotations.get("entities"):
                ner.add_label(ent[2])

        # get names of other pipes to disable them during training
        pipe_exceptions = ["ner", "trf_wordpiecer", "trf_tok2vec"]
        other_pipes = [pipe for pipe in nlp.pipe_names if pipe not in pipe_exceptions]
        with nlp.disable_pipes(*other_pipes):  # only train NER
            # reset and initialize the weights randomly – but only if we're
            # training a new model
            if model is None:
                nlp.begin_training()
            for _i in range(n_iter):
                random.shuffle(TRAIN_DATA)
                losses = {}
                # batch up the examples using spaCy's minibatch
                batches = minibatch(TRAIN_DATA, size=compounding(4.0, 32.0, 1.001))
                for batch in batches:
                    texts, annotations = zip(*batch)
                    nlp.update(
                        texts,  # batch of texts
                        annotations,  # batch of annotations
                        drop=0.5,  # dropout - make it harder to memorise data
                        losses=losses,
                    )
                logging.info(f"Losses {losses}")

        # test the trained model
        for text, _ in TRAIN_DATA:
            doc = nlp(text)
            logging.info(f"Entities {[(ent.text, ent.label_) for ent in doc.ents]}")
            logging.info(f"Tokens {[(t.text, t.ent_type_, t.ent_iob) for t in doc]}")

        # save model to output directory
        if output_dir is not None:
            output_dir = Path(output_dir)
            if not output_dir.exists():
                output_dir.mkdir()
            nlp.to_disk(output_dir)
            logging.info(f"Saved model to {output_dir}")

            # test the saved model
            logging.info(f"Loading from {output_dir}")
            nlp2 = spacy.load(output_dir)
            for text, _ in TRAIN_DATA:
                doc = nlp2(text)
                logging.info(f"Entities {[(ent.text, ent.label_) for ent in doc.ents]}")
                logging.info(f"Tokens {[(t.text, t.ent_type_, t.ent_iob) for t in doc]}")
        


    def load_entity_data(self, limit=0, split=0.8):
        # Partition off part of the train data for evaluation
        train_data = []
        #[ ("E TEMPO DE APRENDER-MEU PRIMEIRO LIVRO (EDUCAÇÃO INFANTIL)", {"entities": [(0, 58, "LIVRO")]}), ]
        return train_data

    def evaluate(self, tokenizer, textcat, texts, cats):
        docs = (tokenizer(text) for text in texts)
        tp = 0.0  # True positives
        fp = 1e-8  # False positives
        fn = 1e-8  # False negatives
        tn = 0.0  # True negatives
        for i, doc in enumerate(textcat.pipe(docs)):
            gold = cats[i]
            for label, score in doc.cats.items():
                if label not in gold:
                    continue
                if label == "NEGATIVE":
                    continue
                if score >= 0.5 and gold[label] >= 0.5:
                    tp += 1.0
                elif score >= 0.5 and gold[label] < 0.5:
                    fp += 1.0
                elif score < 0.5 and gold[label] < 0.5:
                    tn += 1
                elif score < 0.5 and gold[label] >= 0.5:
                    fn += 1
        precision = tp / (tp + fp)
        recall = tp / (tp + fn)
        if (precision + recall) == 0:
            f_score = 0.0
        else:
            f_score = 2 * (precision * recall) / (precision + recall)
        return {"textcat_p": precision, "textcat_r": recall, "textcat_f": f_score}

def getDocLink(doc):
    return doc.get('URL', doc.get('LINK', doc.get('FEED', [''] )))[0]   

def getDocDate(doc):
    return doc.get('DATE', doc.get('DREDATE', doc.get('TIMESTAMP', [''] )))[0]   

def getDocFilters(doc):
    #references = doc.get(f'{filters_fieldprefix}_REFS', [])
    #dbname = doc.get(f'{filters_fieldprefix}_DBS', [])
    links = doc.get(f'{filters_fieldprefix}_LNKS', [])
    prefix = filters_fieldprefix.lower()
    return {
        #f'{prefix}_databases': ','.join(dbname),
        #f'{prefix}_references': ','.join(references),
        f'{prefix}_links': ','.join(links)
    }
