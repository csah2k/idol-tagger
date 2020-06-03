#from __future__ import unicode_literals, print_function
import os
#import re 
import time
import json
import random
import logging
import concurrent.futures
from pathlib import Path
import spacy
from spacy.util import minibatch, compounding, decaying
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
        self.numthreads = self.cfg.get('threads', 2)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.numthreads, thread_name_prefix='SpacyPool')
        self.running = self.executor.submit(self.initService).result()
            
            
    def initService(self):
        try:
            self.loaded_models = {} ## LOAD ALL CUSTOM MODELS
            _, model_dir, _ = util.getDataFilename(self.cfg, "projects")
            for proj_id in os.listdir(model_dir):
                for lang in os.listdir(os.path.join(model_dir, proj_id)):
                    model =  os.path.abspath(os.path.join(model_dir, proj_id, lang))
                    model_name = f"{proj_id}/{lang}"
                    self.logging.info(f"loading model '{model_name}' in '{model}' ...")
                    self.loaded_models[model_name] = spacy.load(model)  
                    self.logging.info(f"model '{model_name}' loaded: {self.loaded_models[model_name]}")

            self.logging.info(f"SpacyNlp service started [threads: {self.numthreads}, models: {len(self.loaded_models.keys())}]")
            return True
        except Exception as error:
            self.logging.error(f"SpacyNlp: {error}")
            return False


    def apply_project_model(self, username:str, proj_id:str, text:str, lang=None):
        return self.executor.submit(self._apply_project_model, username, proj_id, text, lang).result()

    def _apply_project_model(self, username:str, proj_id:str, text:str, lang=None):
        
        # TODO check if user has any access to the project in mongo_role_mappings
        if lang == None:
            lang = self.index.detect_language(text)
            self.logging.debug(f"language detected: {lang}")
        model_name = f"{proj_id}/{lang}"
        self.logging.info(f"Exec model '{model_name}': {text}")

        if model_name not in self.loaded_models.keys():
            err = f"model '{model_name}' not loaded"
            self.logging.warn(err)
            return { 'error': err }
        
        res = { }
        doc = self.loaded_models[model_name](text)
        if hasattr(doc, 'cats'):
            res['cats'] = doc.cats

        return res

        


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
    #   <export_field>: JSON(),
    #   <import_ts_field>: long,
    #   <export_ts_field>: long,
    #   <train_ts_field>: long
    # }

    def run_training_task(self, task:dict):
        return self.executor.submit(self._run_training_task, task).result()

    def _run_training_task(self, task:dict):
        # iterate all project id
        for proj in self.mongo_projects.find({}):
            # get project info
            proj_id = proj['id']
            train_ts_field = f"train_ts_prj_{proj_id}"
            export_ts_field = f"export_ts_prj_{proj_id}"
            export_field = f"export_prj_{proj_id}"
            proj['train_ts_field'] = train_ts_field
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
            #self.logging.info(f"generating proj_{proj_id} training data, indices: {indices}, labels: {proj['labels']}")
            ## TODO support other projects/model types 
            proj_type = proj['project_type'][0]
            if proj_type == "DocumentClassification":
                self.train_classifier_model(task, proj)
            if proj_type == "SequenceLabeling":
                self.train_ner_model(task, proj)

    def train_ner_model(self, task, proj):
        n_iter=100
        languages = self.cfg.get('languages',{})
        self.logging.debug(f"languages: {languages}")
        for lang, model in languages.items():
            # data sample query selection
            proj['index_query'] = util.createTrainDataQuery(proj, lang)
            # load correct dataset from index
            train_data = self.generate_ner_data(proj)
            if len(train_data) == 0:
                # only load the model if is data to train
                self.logging.debug(f"No new training data found in index for language: {lang}")
                continue

            """Load the model, set up the pipeline and train the entity recognizer."""
            # check if already exists the taget model file, if so then load it, and update this existing model
            model_file, _, _ = util.getDataFilename(self.cfg, f"{proj['id']}/{lang}", None, None)
            _nlp = None
            if os.path.exists(model_file): 
                self.logging.info(f"Loading project model '{model_file}'")
                _nlp = spacy.load(model_file)
            else:  # fallback to language core model
                self.logging.info(f"Loading default lang model '{model}'")
                _nlp = spacy.load(model)

            # create the built-in pipeline components and add them to the pipeline
            # nlp.create_pipe works for built-ins that are registered with spaCy
            if "ner" not in _nlp.pipe_names:
                ner = _nlp.create_pipe("ner")
                _nlp.add_pipe(ner, last=True)
            # otherwise, get it so we can add labels
            else:
                ner = _nlp.get_pipe("ner")

            # add labels
            for _, annotations in train_data:
                for ent in annotations.get("entities"):
                    ner.add_label(ent[2])

            # get names of other pipes to disable them during training
            pipe_exceptions = ["ner", "trf_wordpiecer", "trf_tok2vec"]
            other_pipes = [pipe for pipe in _nlp.pipe_names if pipe not in pipe_exceptions]
            with _nlp.disable_pipes(*other_pipes):  # only train NER
                # reset and initialize the weights randomly – but only if we're
                # training a new model
                if model is None:
                    _nlp.begin_training()
                for _i in range(n_iter):
                    random.shuffle(train_data)
                    losses = {}
                    # batch up the examples using spaCy's minibatch
                    batches = minibatch(train_data, size=compounding(4.0, 32.0, 1.001))
                    for batch in batches:
                        texts, annotations = zip(*batch)
                        _nlp.update(
                            texts,  # batch of texts
                            annotations,  # batch of annotations
                            drop=0.5,  # dropout - make it harder to memorise data
                            losses=losses,
                        )
                    self.logging.info(f"Losses {losses}")

            # test the trained model
            for text, _ in train_data:
                doc = _nlp(text)
                self.logging.info(f"Entities {[(ent.text, ent.label_) for ent in doc.ents]}")
                self.logging.info(f"Tokens {[(t.text, t.ent_type_, t.ent_iob) for t in doc]}")

            # save the model
            _nlp.to_disk(model_file)
            self.logging.info(f"Saved model to {model_file}")

            # Load the saved model
            self.loaded_models[f"{proj['id']}/{lang}"] = spacy.load(model_file)  
            self.logging.debug(self.loaded_models)


    def train_classifier_model(self, task, proj):
        # TODO buscar essas variaveis dos parametros da task, project ou self.config
        n_iter=20
        n_texts=2000
        init_tok2vec=None
        languages = self.cfg.get('languages',{})
        self.logging.debug(f"languages: {languages}")
        for lang, model in languages.items():
            # data sample query selection
            proj['index_query'] = util.createTrainDataQuery(proj, lang)
            # load correct dataset from index
            (train_texts, train_cats), (dev_texts, dev_cats), categories = self.generate_classifier_data(proj)
            if len(categories) == 0 or len(train_texts) == 0:
                # only load the model if is data to train
                self.logging.debug(f"No new training data found in index for language: {lang}")
                continue
            
            # check if already exists the taget model file, if so then load it, and update this existing model
            model_file, _, _ = util.getDataFilename(self.cfg, f"{proj['id']}/{lang}", None, None)
            _nlp = None
            if os.path.exists(model_file): 
                self.logging.info(f"Loading project model '{model_file}'")
                _nlp = spacy.load(model_file)
            else:  # fallback to language core model
                self.logging.info(f"Loading default lang model '{model}'")
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
                dropout = decaying(0.6, 0.2, 1e-4)
                for _i in range(n_iter):
                    losses = {}
                    # batch up the examples using spaCy's minibatch
                    random.shuffle(train_data)
                    batches = minibatch(train_data, size=batch_sizes)
                    for batch in batches:
                        texts, annotations = zip(*batch)
                        _nlp.update(texts, annotations, sgd=optimizer, drop=next(dropout), losses=losses)
                        #_nlp.update(texts, annotations, sgd=optimizer, drop=0.2, losses=losses)
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

            # save the model
            with _nlp.use_params(optimizer.averages):
                _nlp.to_disk(model_file)
            self.logging.info(f"Saved model to {model_file}")

            # Load the saved model
            self.loaded_models[f"{proj['id']}/{lang}"] = spacy.load(model_file)  
            self.logging.debug(self.loaded_models)
            


    # TODO
    def generate_ner_data(self, proj:dict, minhits=10, limit=100, split=0.8):
        # defaults: "LOC","MISC","ORG","PER"
        export_field = proj['export_field']
        index_query = proj['index_query']
        indices = proj['indices']      
        train_data = []
        # parse documents into expected 'spacy' format
        for hit in self.index.query(index_query, indices):
            hit_data = hit.get(export_field,{})
            self.logging.info(f"hit_data: {hit_data}")
            # trasformar disso:  [{'end_offset': 147, 'label': 5, 'start_offset': 136, 'user': 1}, ... ]
            #       nisto aqui:  [ ("E TEMPO DE APRENDER-MEU PRIMEIRO LIVRO (EDUCAÇÃO INFANTIL)", {"entities": [(0, 58, "LIVRO")]}), ... ]

            train_data.append(hit_data) 
        
        #if len(train_data) >= minhits:

        return train_data

    def generate_classifier_data(self, proj:dict, minhits=10, limit=100, split=0.8):
        export_field = proj['export_field']
        index_query = proj['index_query']
        indices = proj['indices']      
        train_data = []
        # parse documents into expected 'spacy' format
        for hit in self.index.query(index_query, indices):
            hit_data = hit.get(export_field,{})
            labels = [proj['labels'][str(_l.get('label'))] for _l in hit_data]
            train_data.append(( (hit['id'],hit['index']) , hit['content'], labels)) 
        
        if len(train_data) >= minhits:
            random.shuffle(train_data)
            train_data = train_data[-limit:]
            ids, texts, labels = zip(*train_data)
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
            
            # mark train data used in the index
            selected_ids = ids[:split]
            #self.logging.info(f"selected_ids: {selected_ids}")
            curr_time = int(time.time())
            for _id, _index in selected_ids:
                _res = self.index.update_fields(_index, _id, { proj['train_ts_field']: curr_time })
                if (_res or {}).get('result',None) != 'updated':
                    self.logging.error(f"Error updating index field '{proj['train_ts_field']}' to {curr_time} -> {_res}")

            return (texts[:split], cats[:split]), (texts[split:], cats[split:]), list(labels_template.keys())
        return ([], []), ([], []), []

    
    


            

    
        

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

   
