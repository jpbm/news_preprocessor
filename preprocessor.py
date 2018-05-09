import json
import sys
sys.path.append('../')
sys.path.append('~/waxonwaxoff')
sys.path.append('/')

import re
from bs4 import BeautifulSoup
import os
from src.util import count_lines_many, data_from_many, bunch_paragraphs, unpack


#print(sys.argv)
FN = sys.argv[1].split('.')[1]
DATAPATH = '/datapool/news_articles/raw_data/'+FN+'/'
DESTFILE = '/datapool/news_articles/'+FN+'/'+FN+'.json'
MAX_PER_FILE = 1000

print(FN)
print(DATAPATH)
print(DESTFILE)
try:
    os.mkdir('/datapool/news_articles/'+FN)
except:
    pass

"""GENERAL"""

print("running...")
print("Counting number of entries...")
datafiles = [DATAPATH + file for file in os.listdir(DATAPATH)]
print("Number of entries: %i" % (count_lines_many(datafiles)))


"""ATTEMPTED GENERAL"""

def preprocess(item):
    url, html = unpack(item)
    label = get_label(url,html)
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


nytimes_url_label_re = re.compile('(?<=https://www.nytimes.com/[12][0-9][0-9][0-9]/[0-1][0-9]/[0-3][0-9]/).*')
usatoday_url_label_re = re.compile('(?<=https://www.usatoday.com/).*(?=/[12][0-9][0-9][0-9])')

def get_url_label(url):
    if 'nytimes.com' in url:
        return ['/'.join(nytimes_url_label_re.search(url).group().split('/')[:-1])]
    elif 'usatoday' in url:
        return usatoday_url_label_re.findall(url)
    else:
        return []

label_re = re.compile('(?<=<meta name="prism.section" content=")[^"]*(?=">)')
def get_label(url,html):
    soup = BeautifulSoup(html,'html.parser')
    regex_label = label_re.findall(html) #[item['content'] for item in soup.findAll('meta',content=True,property=True) if item['property'] == 'article:section']
    url_label = get_url_label(url)
    soup_label_0 = [item['content'] for item in soup.findAll('meta',content=True,property=True) if item['property'] == 'article:section' or item['property'] =='og:section']
    soup_label_1 = [item['content'] for item in soup.findAll('meta',content=True,property=True) if item['property'] == 'og:site_name']
    label = regex_label + soup_label_0 + url_label + soup_label_1
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
                    print("%i datapoints written... %s\t%s" % (i,item[0],FN) + 50*' ',end='\n')
                    file.write(json.dumps(item)+'\n')
                    if i % MAX_PER_FILE == 0:
                        j+=1
                        filename = DESTFILE.split('.')[0]+str(j) + '.json'
                        file.close()
                        file = open(filename,'w')
    file.close()
    print()
    print("%s processes. %i lines written to %s." % (DATAPATH,i,DESTFILE)) 
