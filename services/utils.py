import os
import html
import html2text
from requests.structures import CaseInsensitiveDict

FIELDPREFIX_FILTER = 'FILTERINDEX'
FIELDSUFFIX_TAGGED = '_TAGGED'
FIELDSUFFIX_TRAINED = '_TRAINED'
DFLT_LANGUAGE = 'GENERAL'
DFLT_ENCODE = 'UTF8'

## --------- helper functions ------------
def makeUrl(component):
    return f"{component.get('protocol','http')}://{component.get('host','localhost')}:{component.get('port',9000)}"

def aciQuery(query, modify={}):
    _q = CaseInsensitiveDict(query)   
    _q.pop('a', None)
    _q.pop('action', None)
    _q.pop('actionid', None)
    _q.pop('responseformat', None)
    _q.update(modify)
    return _q

def hashDict(dct):
    return str(hash(frozenset(dct.items())))

def cleanText(text):
    text_maker = html2text.HTML2Text()
    text_maker.ignore_links = True
    text_maker.ignore_images = True
    text = html.unescape(text)
    text = text_maker.handle(text)
    text = text.strip().capitalize()
    return text

def getDocLink(doc):
    return doc.get('URL', doc.get('LINK', doc.get('FEED', [''] )))[0]   

def getDocDate(doc):
    return doc.get('DATE', doc.get('DREDATE', doc.get('TIMESTAMP', [''] )))[0]   

def getDocFilters(doc):
    #references = doc.get(f'{FIELDPREFIX_FILTER}_REFS', [])
    #dbname = doc.get(f'{FIELDPREFIX_FILTER}_DBS', [])
    links = doc.get(f'{FIELDPREFIX_FILTER}_LNKS', [])
    #prefix = FIELDPREFIX_FILTER.lower()
    return {
        #f'{prefix}_databases': ','.join(dbname),
        #f'{prefix}_references': ','.join(references),
        f'LINKS': ','.join(doc.get(f'{FIELDPREFIX_FILTER}_LNKS', []))
    }

def getProjectLastRuntime(project, db):
    table = db['executions']
    return table.find(order_by='-runtime', _limit=1)
    #for row in results:
    #    print(f"row['runtime']")

def getDataFilename(config, project, sufx=None, ext='dat', trunc=False, delt=False):
        datafile = None
        if sufx != None: datafile = f"{project.get('name')}_{sufx}.{ext}"
        else: datafile = f"{project.get('name')}.{ext}"
        dataFolder = config.get('tempfolder', 'data')
        target_file = os.path.abspath(os.path.join(dataFolder, datafile))
        target_folder = os.path.dirname(target_file)
        os.makedirs(target_folder, exist_ok=True)
        if trunc: open(target_file, 'w').close()
        if delt and os.path.exists(target_file): os.remove(target_file)
        return target_file, target_folder, os.path.basename(target_file)