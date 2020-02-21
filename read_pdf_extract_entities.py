import spacy
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from tika import parser
import pandas as pd

from textblob import TextBlob

#read pdf file
raw = parser.from_file("/home/neilbyrne/Documents/Code/Project StudyMuffin/coulomb.pdf")
meta = raw['metadata']


content = str(raw['content'])
author = str(meta['Author'])
title = "Pt 6: " + str(meta['title'])

#enocode raw data
content = content.encode('utf-8', errors='ignore')

#clean data (needs work)
content = str(content).replace("\n", "").replace("\\", "")

#extract noun entities
entities = []

def elementcontainsgarbage(token):
    garbage = False
    if token.find('xf0') >=0 or token.find('xcex') >=0 or token.find('xe2x') >=0 or token.find('+n') >= 0:
        garbage = True
    return garbage

# ====== Connection ====== #
# Connection to ElasticSearch
es = Elasticsearch(['http://localhost:9220'],timeout=600)

# cols = ['title', 'author', 'text', 'keywords']
cols = ['keyword']

pdf_data = pd.DataFrame(columns = cols)

#try TextBlob
textblob_lecture = TextBlob(content)
print(textblob_lecture.noun_phrases)
# print (textblob_lecture.sentences)

for element in textblob_lecture.tags:
    # if element not in entities:
    #     print(element)
    #     entities.append(element)
    #     pdf_data = pdf_data.append({'keyword': element}, ignore_index=True)
    if str(element[1]).find('NNP') >= 0 and element[0] not in entities and len(str(element[0])) > 3 and not elementcontainsgarbage(str(element[0])):
        entities.append(element[0])
        res = es.search(index="keywords", body={"query": {"match": {"keyword": element[0]}}})
        if not len(res['hits']['hits']) > 0:
            pdf_data = pdf_data.append({'keyword': element[0]}, ignore_index=True)


print (entities)
print (len(entities))

# pdf_data = pdf_data.append({'title': title, 'author': author, 'text': content, 'keywords': entities}, ignore_index=True)
lecture = pdf_data.to_dict(orient='records')
# bulk(es, lecture, index='lectures', doc_type='doc', raise_on_error=True)
bulk(es, lecture, index='keywords', doc_type='doc', raise_on_error=True)
