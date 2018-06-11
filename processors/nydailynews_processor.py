import json
import sys
sys.path.append('../')
sys.path.append('/')

import re
from bs4 import BeautifulSoup
import os
from src.util import count_lines_many, data_from_many, bunch_paragraphs, unpack

FN = 'nydailynews'
DATAPATH = '/datapool/news_articles/raw_data/nydailynews/'
DESTFILE = '/datapool/news_articles/nydailynews/nydailynews.json'
MAX_PER_FILE = 1000

"""GENERAL"""

print("Counting number of entries...")
datafiles = [DATAPATH + file for file in os.listdir(DATAPATH)]
print("Number of entries: %i" % (count_lines_many(datafiles)))


def preprocess(item):
    url, html = unpack(item)
    label = get_label(url)
    paragraphs = get_paragraphs(html)
    return label, paragraphs


"""NYDAILYNEWS"""
def get_paragraphs(html):
    soup = BeautifulSoup(html,'html.parser')
    if soup.findAll("article"):
        paragraphs = [tag.get_text().strip('\r\n\t') for tag in 
                      soup.findAll("title")+
                      soup.findAll("article")[0].findAll('p')+
                      soup.findAll("span",itemprop="caption")+
                      soup.findAll("p","g-article-html")]
    else:
        paragraphs = [tag.get_text().strip('\r\n\t') for tag in 
              soup.findAll("title")+
              soup.findAll("span",itemprop="caption")+
              soup.findAll("p","g-article-html")]
        
    if len(paragraphs) > 1:
        return paragraphs
    else:
        return 'NOCONTENT'
    
label_re = re.compile('(?<=http://www.nydailynews.com/).*')
def get_label(url):
    label = label_re.search(url)
    if label:
        return '/'.join(label.group().split('/')[:-1])
    else:
        return "NOLABEL"


###%%% new insertion
def filter_paragraphs(FN,paragraphs):
    filtered_paragraphs = []
    
    forbidden = 'Copyright ©,New York Daily News,on a mobile device?,Get our instant notifications as news happens,You can manage them anytime using browser settings,Sign up now to start receiving breaking news alerts on your desktop'.split(',')
    
    for paragraph in paragraphs:
        res = True
        
        if len(paragraph.split(' ')) <= 4:
            res = False
        else:
            pass
        
        for item in forbidden:
            if item.lower() in paragraph.lower():
                res = False
            else:
                pass
                
        if paragraph[:3] == 'By ':
            res = False
        else:
            pass
            
        if paragraph[:7] == 'Follow ':
            res = False
        else:
            pass
        
        if paragraph.isupper():
            res = False
        else:
            pass
        
        if res:
            filtered_paragraphs.append(paragraph)
                
    return filtered_paragraphs[1:]







print("Writing data...")
if __name__ == "__main__":
    data = map(preprocess,data_from_many(datafiles))
    i = 0
    j = 0
    filename = DESTFILE.split('.')[0] + str(j) + '.json'
    file = open(filename,'w')
    for label, paragraphs in data:
        if label != 'NOLABEL' and paragraphs != 'NOCONTENT':
            try:
                text_chunks = bunch_paragraphs(filter_paragraphs(FN,paragraphs),target_length=250)
                labels = [label for i in range(len(text_chunks))]
                items = zip(labels,text_chunks)
                for item in items:
                    if len(item[1].split(' ')) >= 100:
                        i+=1
                        print("%i datapoints written... %s\t\t\t\t\t" % (i,item[0]),end='\r')
                        file.write(json.dumps(item)+'\n')
                        if i % MAX_PER_FILE == 0:
                            j+=1
                            filename = DESTFILE.split('.')[0]+str(j) + '.json'
                            file.close()
                            file = open(filename,'w')
            except:
                print("ERROR")
    file.close()
    print()
    print("%s processes. %i lines written to %s." % (DATAPATH,i,DESTFILE)) 
