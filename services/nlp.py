from __future__ import unicode_literals, print_function
import re 
import csv
import json
import plac
import random
import codecs
import logging
import datetime
import concurrent.futures
from retrying import retry
from pathlib import Path
import thinc.extra.datasets
import spacy
from spacy.util import minibatch, compounding

TRAIN_DATA = []


# https://github.com/doccano/doccano
# https://github.com/doccano/doccano/issues/299

class Service:

    executor = None
    logging = None
    idol = None

    def __init__(self, logging, threads, idol): 
        self.logging = logging
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=threads)
        self.idol = idol

    def export_training_sentiment_jsonl(self, text):
        with codecs.open('data/training_sentiment.jsonl', 'a', 'utf-8') as outfile:
            idolQuery = {
                'DatabaseMatch' : 'RSS_FEEDS',
                'MinScore' : 30,
                'Text' : text
            }
            hits = self.idol.query(idolQuery)

            #{"text": "Great price.", "labels": ["positive"]}
            for hit in hits:
                # TODO create labels from fields NEGATIVE_PARAM and POSITIVE_PARAM
                labels = []
                jsonl = json.dumps({"text": hit.get('title'), "labels": labels}, ensure_ascii=False).encode('utf8')
                outfile.write(jsonl.decode()+'\n')
                
    def export_training_ner_jsonl(self, text):
        with open('data/training_ner.jsonl', 'w') as outfile:
            idolQuery = {
                'DatabaseMatch' : 'RSS_FEEDS',
                'MinScore' : 30,
                'Text' : 'Apple'
            }
            hits = self.idol.query(idolQuery)

            #{"text": "President Obama", "labels": [ [10, 15, "PERSON"] ]}
            for hit in hits:
                json.dump({"text": "Great price.", "labels": ["positive"]}, outfile)

            
            
            
            

    
           
             





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
        


    @plac.annotations(
        model=("Model name. Defaults to blank 'en' model.", "option", "m", str),
        output_dir=("Optional output directory", "option", "o", Path),
        n_texts=("Number of texts to train from", "option", "t", int),
        n_iter=("Number of training iterations", "option", "n", int),
        init_tok2vec=("Pretrained tok2vec weights", "option", "t2v", Path),
    )
    def train_model_sentiment(self, model=None, output_dir=None, n_iter=20, n_texts=2000, init_tok2vec=None):    
        if model is not None:
            nlp = spacy.load(model)  # load existing spaCy model
            logging.info("Loaded model '%s'" % model)
        else:
            nlp = spacy.blank("en")  # create blank Language class
            logging.info("Created blank 'en' model")
        # add the text classifier to the pipeline if it doesn't exist
        # nlp.create_pipe works for built-ins that are registered with spaCy
        if "textcat" not in nlp.pipe_names:
            textcat = nlp.create_pipe(
                "textcat", config={"exclusive_classes": True, "architecture": "simple_cnn"}
            )
            nlp.add_pipe(textcat, last=True)
        # otherwise, get it, so we can add labels to it
        else:
            textcat = nlp.get_pipe("textcat")

        # add label to text classifier
        textcat.add_label("POSITIVE")
        textcat.add_label("NEGATIVE")

        # load the IMDB dataset
        logging.info("Loading sementiment data...")
        (train_texts, train_cats), (dev_texts, dev_cats) = self.load_sentiment_data()
        train_texts = train_texts[:n_texts]
        train_cats = train_cats[:n_texts]
        logging.info(
            "Using {} examples ({} training, {} evaluation)".format(
                n_texts, len(train_texts), len(dev_texts)
            )
        )
        train_data = list(zip(train_texts, [{"cats": cats} for cats in train_cats]))

        # get names of other pipes to disable them during training
        pipe_exceptions = ["textcat", "trf_wordpiecer", "trf_tok2vec"]
        other_pipes = [pipe for pipe in nlp.pipe_names if pipe not in pipe_exceptions]
        with nlp.disable_pipes(*other_pipes):  # only train textcat
            optimizer = nlp.begin_training()
            if init_tok2vec is not None:
                with init_tok2vec.open("rb") as file_:
                    textcat.model.tok2vec.from_bytes(file_.read())
            logging.info("Training the model...")
            logging.info("{:^5}\t{:^5}\t{:^5}\t{:^5}".format("LOSS", "P", "R", "F"))
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
                print(
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
        logging.info(test_text, doc.cats)

        if output_dir is not None:
            with nlp.use_params(optimizer.averages):
                nlp.to_disk(output_dir)
            logging.info(f"Saved model to {output_dir}")

            # test the saved model
            logging.info(f"Loading from {output_dir}")
            nlp2 = spacy.load(output_dir)
            doc2 = nlp2(test_text)
            logging.info(test_text, doc2.cats)


    def load_entity_data(self, limit=0, split=0.8):
        # Partition off part of the train data for evaluation
        train_data = []
        #[ ("E TEMPO DE APRENDER-MEU PRIMEIRO LIVRO (EDUCAÇÃO INFANTIL)", {"entities": [(0, 58, "LIVRO")]}), ]
        with open('data/entity.csv', newline='\n') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='"', skipinitialspace=True)
            for row in spamreader:
                text = row[3]
                label = int(row[2])
                train_data.append((text, label)) 

        return train_data

    def load_sentiment_data(self, limit=0, split=0.8):
        # Partition off part of the train data for evaluation
        train_data = []
        with open('data/sentiment.csv', newline='\n') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='"', skipinitialspace=True)
            for row in spamreader:
                text = row[3]
                label = int(row[2])
                train_data.append((text, label)) 
        random.shuffle(train_data)
        train_data = train_data[-limit:]
        texts, labels = zip(*train_data)
        #print(labels)
        cats = [{"POSITIVE": bool(y == 1), "NEGATIVE": not bool(y == 1)} for y in labels]
        split = int(len(train_data) * split)
        return (texts[:split], cats[:split]), (texts[split:], cats[split:])

    def load_data_imdb(self, limit=0, split=0.8):
        """Load data from the IMDB dataset."""
        # Partition off part of the train data for evaluation
        train_data, _ = thinc.extra.datasets.imdb()
        random.shuffle(train_data)
        train_data = train_data[-limit:]
        texts, labels = zip(*train_data)
        cats = [{"POSITIVE": bool(y), "NEGATIVE": not bool(y)} for y in labels]
        split = int(len(train_data) * split)
        return (texts[:split], cats[:split]), (texts[split:], cats[split:])

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