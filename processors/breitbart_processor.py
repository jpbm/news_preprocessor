import json
import sys
sys.path.append('../')
sys.path.append('~/waxonwaxoff')
sys.path.append('/')

import re
from bs4 import BeautifulSoup
import os
from src.util import count_lines_many, data_from_many, bunch_paragraphs, unpack

DATAPATH = '/datapool/news_articles/raw_data/breitbart/'
DESTFILE = '/datapool/news_articles/breitbart/breitbart.json'
MAX_PER_FILE = 1000

"""GENERAL"""

print("running...")
print("Counting number of entries...")
datafiles = [DATAPATH + file for file in os.listdir(DATAPATH)]
print("Number of entries: %i" % (count_lines_many(datafiles)))


"""BREITBART"""

def preprocess(item):
    url, html = unpack(item)
    label = get_label(html)
    paragraphs = get_paragraphs(html)
    return label, paragraphs


def get_paragraphs(html):
    soup = BeautifulSoup(html,'html.parser')
    paragraphs = [
            tag.get_text() for tag in soup.findAll('title')]+[
                    item['content'] for item in soup.findAll('meta',property=True) if item['property'] == 'og:description']+[
                            tag.get_text() for tag in soup.findAll('p')]
    if len(paragraphs) != 0:
        return paragraphs
    else:
        print("NOCONTENT")
        return 'NOCONTENT'

def get_label(html):
    soup = BeautifulSoup(html,'html.parser')
    label = [item['content'] for item in soup.findAll('meta',content=True,property=True) if item['property'] == 'article:section']
    if len(label) != 0:
        return label[0]
    else:
        print('NOLABEL')
        return 'NOLABEL'

print("Writing data...")
if __name__ == "__main__":
    data = map(preprocess,data_from_many(datafiles))
    #print("data generator created")    
    i = 0
    j = 0
    k = 0
    filename = DESTFILE.split('.')[0] + str(j) + '.json'
    file = open(filename,'w')
    #print("file created %s" % filename) 
    for label, paragraphs in data:
        k+=1
        #print('%i' % k)
        #print("label, paragraph extracted",end = '\r')
        if label != 'NOLABEL' and paragraphs != 'NOCONTENT':
            text_chunks = bunch_paragraphs(paragraphs,target_length=250)
            labels = [label for i in range(len(text_chunks))]
            items = zip(labels,text_chunks)
            for item in items:
                if len(item[1].split(' ')) >= 100:
                    i+=1
                    print("%i datapoints written... %s" % (i,item[0]) + 50*' ',end='\n')
                    file.write(json.dumps(item)+'\n')
                    if i % MAX_PER_FILE == 0:
                        j+=1
                        filename = DESTFILE.split('.')[0]+str(j) + '.json'
                        file.close()
                        file = open(filename,'w')
    file.close()
    print()
    print("%s processes. %i lines written to %s." % (DATAPATH,i,DESTFILE)) 
