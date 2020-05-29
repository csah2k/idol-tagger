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

import services.doccano as doccano
import services.utils as util

# https://spacy.io/api


class Service:

    def __init__(self, logging, config, idol): 
        self.logging = logging 
        self.config = config.get('spacynlp').copy()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.config.get('threads', 4), thread_name_prefix='SpacyPool')
        self.doccano = doccano.Service(logging, config, idol)
        self.idol = idol 


    
    def generate_training_data(self, task:dict):
        # get project info
        proj_id = task['projectid']
        proj = self.mongo_projects.find_one({"id":proj_id})
        proj_type = proj['project_type'][0]
        if proj_type == "DocumentClassification":
            self.generate_training_doc_class(task, proj)
        # TODO other types
    

    def generate_training_doc_class(self, task:dict, project:dict):
        # get project labels
        proj_id = project['id']
        proj_labels = {}
        for label in self.mongo_labels.find({"project":[proj_id]}):
            proj_labels[label.get('id')] = label.get('text')
        #self.logging.info(f"proj_labels: {proj_labels}")

        lookup = { 
            "from": "document_annotations",
            "localField": "id",
            "foreignField": "document",
            "as": "annotations"
        }
        #self.logging.info(f"lookup: {lookup}")
        for doc in self.mongo_documents.aggregate([
                {"$lookup": lookup },
                {"$match": {"project":[proj_id], "annotations": {"$exists": True, "$ne": []}  } },
                #{'$sort': { sort: order } }
            ]):
            self.logging.info(f"doc to export: {doc}")
            # TODO output somewhere in the correct format
            

        
    def openProjectDB(self, project):        
        dbfile, _, _ = util.getDataFilename(self.config, project, 'sqlite', 'db')
        return dataset.connect(f'sqlite:///{dbfile}')
    
    def train_project_model(self, project):  
        return self.executor.submit(self._train_project_model, project)

    def _train_project_model(self, project):  
        self.logging.info(f"==== Training model ====>  ({project.get('project')})")  
        _project = self.doccano.populateProject(project)
        projectType = _project.get('project_type').lower()
        if projectType == 'documentclassification':
            self.train_model_classifier(_project)
        elif projectType == 'sequencelabeling':
            self.train_model_ner(_project)
        elif projectType == 'seq2seq':
            self.train_model_sql(_project)
        else:
            self.logging.error(f"Doccano project_type not expected: {projectType}")

    def train_model_classifier(self, project, model=None, output_dir=None, n_iter=20, n_texts=2000, init_tok2vec=None):  
        languages = self.config.get('languages',{})
        self.logging.info(f"languages: {languages}")
        for lang, model in languages.items():
            # load correct dataset from index
            self.logging.info(f"Loading classifier data for language: {lang}")
            (train_texts, train_cats), (dev_texts, dev_cats), categories = self.load_classifier_data(project, lang)
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
            test_text = "Aggressive treatment against covid war in all countries"
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
                
    def load_classifier_data(self, project, language, limit=100, split=0.8):
        train_data = []
        # query documents from this project database in IDOL
        query = {
            'DatabaseMatch': project.get('database'),
            'FieldText': f"TERM{{label}}:{project.get('datafield')}", ## tagged but not trained
            'PrintFields': f"{project.get('datafield')},{project.get('textfield')}",
            'AnyLanguage': False,
            'MatchLanguageType': language,
            'MaxResults': limit,
            'Text': '*'
        }
        hits = self.idol.query(query)
        self.logging.info(f"hits: {len(hits)}")

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
            project_labels = self.doccano.get_label_list(project.get('id'))
            labels_map = {}
            labels_template = {}
            for _l in project_labels:
                labels_map[_l.get('id')] = _l.get('text')
                labels_template[_l.get('text')] = False

            self.logging.info(f"labels_map: {labels_map}")

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
        return ([], []), ([], []), []

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


    def train_model_sql(self, project):
        self.logging.wanr(f"not implemented")

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

   
