#from __future__ import unicode_literals, print_function
import os
#import re 
import json
import random
import logging
import concurrent.futures
from pathlib import Path
import spacy
from spacy.util import minibatch, compounding
from requests.structures import CaseInsensitiveDict

from services.elastic import Service as elasticService
import services.utils as util

# https://spacy.io/api


class Service:

    def __init__(self, logging, config, mongodb, index:elasticService): 
        self.running = False
        self.logging = logging 
        self.config = config
        self.tasks_defaults = config.get('tasks_defaults',{})
        self.cfg = config['spacynlp'].copy()
        self.index = index 
        self.mongo_tasks = mongodb['tasks']
        self.mongo_users = mongodb['users']
        self.mongo_roles = mongodb['roles']
        self.mongo_labels = mongodb['labels']
        self.mongo_projects = mongodb['projects']
        self.mongo_role_mappings = mongodb['role_mappings']
        numthreads = self.cfg.get('threads',2)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=numthreads, thread_name_prefix='SpacyPool')
        try:
            self.logging.info(f"SpacyNlp service started [threads: {numthreads}]")
            self.running = True
        except Exception as error:
            self.logging.error(f"cant start SpacyNlp service, error -> {str(error)}")

    ############## INDEX HIT ##############
    # {  
    #   'id': '852fa73', 
    #   'index': 'f628d93e02ab48b5ad5', 
    #   'score': 100.0, 
    #   'date': '2020-05-29T18:16:30.000Z', 
    #   'src': 'http://rss.cnn.com/rss/edition_us.rss', 
    #   'task_id': '5ed45f1c5ea2', 
    #   'language': 'en', 
    #   'title': 'This incredibly', 
    #   'content': 'Tj maxx built its appeal on the "treasure hunt" shopping experience.', 
    #   'url': 'http://rss.cnn.com/~r/rss/edition_us/~3/eifFUyevAw0/index.html', 
    #   'indextask': 'Feeds de noticias',
    #   'filter': JSON(), 
    #   <export_field>: JSON()
    # }
    
    def run_training_task(self, task:dict):
        # get project info
        proj_id = task['params']['projectid']
        proj = self.mongo_projects.find_one({"id":proj_id})
        export_ts_field = f"export_ts_prj_{proj_id}"
        export_field = f"export_prj_{proj_id}"
        proj['export_ts_field'] = export_ts_field
        proj['export_field'] = export_field
        # get project labels
        proj['labels'] = {}
        for label in self.mongo_labels.find({"project":[proj_id]}):
            proj['labels'][label.get('id')] = label.get('text')
        # get admin users indices
        users = proj.get('users', [])
        indices = ','.join([_u['indices']['indexdata'] for _u in self.mongo_users.find({"id":{"$in":users}})])
        proj['indices'] = indices
        self.logging.info(f"generating proj_{proj_id} training data, indices: {indices}, labels: {proj['labels']}")

        ## TODO support other projects/model types 
        proj_type = proj['project_type'][0]
        if proj_type == "DocumentClassification":
            self.train_classifier_model(task, proj)
        if proj_type == "SequenceLabeling":
            self.train_ner_model(task, proj)

    # TODO
    def generate_ner_data(self, proj:dict):
        #_export_field = proj['export_field']
        #_index_query = proj['index_query']
        #_indices = proj['indices']
        return None

    def generate_classifier_data(self, proj:dict):
        export_field = proj['export_field']
        index_query = proj['index_query']
        indices = proj['indices']
        # TODO buscar essas variaveis dos parametros do project ou do config
        limit=100
        split=0.8
        train_data = []
        # parse documents into expected 'spacy' format
        for hit in self.index.query(index_query, indices):
            self.logging.info(f"hit: {hit}")
            hit_data = hit.get(export_field,{})
            labels = [proj['labels'][str(_l.get('label'))] for _l in hit_data]
            train_data.append((hit['content'], labels)) 
        if len(train_data) > 1:
            random.shuffle(train_data)
            train_data = train_data[-limit:]
            texts, labels = zip(*train_data)
            # list categories (labels) from Doccano
            labels_template = {}
            for _l, text in proj['labels'].items():
                labels_template[text] = False
            # extract categories
            cats = []
            for _lbls in labels:
                cat = labels_template.copy()
                for _l in _lbls:
                    cat.update({_l:True})
                cats.append(cat)
            # Partition off part of the train data for evaluation
            split = int(len(train_data) * split)
            return (texts[:split], cats[:split]), (texts[split:], cats[split:]), list(labels_template.keys())
        return ([], []), ([], []), []

    # TODO
    def train_ner_model(self, task, proj):
        return self.generate_ner_data(proj)

    def train_classifier_model(self, task, proj):
        # TODO buscar essas variaveis dos parametros da task, project ou self.config
        model=None
        output_dir=None
        n_iter=20
        n_texts=2000
        init_tok2vec=None
        languages = self.cfg.get('languages',{})
        self.logging.info(f"languages: {languages}")
        for lang, model in languages.items():
            # data sample query selection
            index_query = {
                "query": {
                    "bool" : {
                        "must" : {
                            "range" : {
                                proj['export_ts_field'] : {
                                    "gte" : 10, #task.get('lastruntime',10) # TODO DEBUG 
                                }
                            }
                        },
                        "filter": {
                            "term" : { "language": lang }
                        }
                    }
                }
            }
            proj['index_query'] = index_query
            self.logging.info(f"index_query: {proj['index_query']}")
            # load correct dataset from index
            self.logging.info(f"Loading classifier data for language: {lang}")
            (train_texts, train_cats), (dev_texts, dev_cats), categories = self.generate_classifier_data(proj)
            if len(categories) == 0 or len(train_texts) == 0:
                self.logging.info("No new training data found in index")
                continue

            # only load the model if is data to train
            self.logging.info("Loading model '%s'" % model)
            _nlp = spacy.load(model)
            # add the text classifier to the pipeline if it doesn't exist
            if "textcat" not in _nlp.pipe_names:
                textcat = _nlp.create_pipe("textcat", config={"exclusive_classes": True, "architecture": "simple_cnn"})
                _nlp.add_pipe(textcat, last=True)
            # otherwise, get it, so we can add labels to it
            else:
                textcat = _nlp.get_pipe("textcat")

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
            other_pipes = [pipe for pipe in _nlp.pipe_names if pipe not in pipe_exceptions]
            with _nlp.disable_pipes(*other_pipes):  # only train textcat
                optimizer = _nlp.begin_training()
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
                        _nlp.update(texts, annotations, sgd=optimizer, drop=0.2, losses=losses)
                    with textcat.model.use_params(optimizer.averages):
                        # evaluate on the dev data split off in load_data()
                        scores = self.evaluate(_nlp.tokenizer, textcat, dev_texts, dev_cats)
                    self.logging.debug(
                        "{0:.3f}\t{1:.3f}\t{2:.3f}\t{3:.3f}".format(  # print a simple table
                            losses["textcat"],
                            scores["textcat_p"],
                            scores["textcat_r"],
                            scores["textcat_f"],
                        )
                    )

            # test the trained model
            test_text = "Social restrictions to fight covid virus benefits lula that was arrested in jail"
            doc = _nlp(test_text)
            best_cat = ('', 0)
            for _cat in doc.cats:
                if doc.cats[_cat] > best_cat[1]:
                    best_cat = (_cat, doc.cats[_cat])
            self.logging.info(f"{best_cat} : {test_text}")

            if output_dir is not None:
                with _nlp.use_params(optimizer.averages):
                    _nlp.to_disk(output_dir)
                self.logging.info(f"Saved model to {output_dir}")

                # test the saved model
                self.logging.info(f"Loading from {output_dir}")
                nlp2 = spacy.load(output_dir)
                doc2 = nlp2(test_text)
                self.logging.info(test_text, doc2.cats)
    


            

    def train_model_ner(self, model=None, output_dir=None, n_iter=100):
        TRAIN_DATA = [] # TODO

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

   
