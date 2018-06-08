"""
PARAGRAPH + LABEL EXTRACTION

Paths are configured for current setup on toymaker.

Tested for:
    nytimes
    usatoday
    foxnews
    theonion
    buzzfeed
    nypost
    nydailynews
    washingtonpost
    wsj



"""


import json
import sys
import re
from bs4 import BeautifulSoup
import os
from src.util import count_lines_many, data_from_many, bunch_paragraphs, unpack


"""PREPROCESS AND PARAGRAPH EXTRACTION"""

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


def filter_paragraphs(FN,paragraphs):
    filtered_paragraphs = []
    
    if FN == 'nytimes':
        forbidden = 'New York Times,|,not a robot,Invalid email address,feedback on this page,Home Page,@nytimes.com,NYTimes.com,contributed reporting.,newsletter.,updates via Facebook'.split(',')
    elif FN == 'wsj':
        forbidden = 'WSJ,All Rights Reserved,The Wall Street Journal,summary.headline,contributed to this article,Photo: ,\n and ,sign in'.split(',')
    elif FN == 'wapo':
        forbidden = 'The Washington Post,Show me more offers,related: ,[,],sign in.,sign up,check your inbox,signed up,you might also like, AM, PM,valid email address,This post has been updated,|'.split(',')
    elif FN == 'nypost':
        forbidden = '|,privacy policy.,other great newsletters,…,information services.,delivered to your inbox,received your submission.,\t\t\t\n'.split(',')
    elif FN == 'nydailynews':
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



"""LABEL EXTRACTION"""

nytimes_url_label_re = re.compile('(?<=nytimes.com/[12][0-9][0-9][0-9]/[0-1][0-9]/[0-3][0-9]/).*')
usatoday_url_label_re = re.compile('(?<=usatoday.com/)[a-zA-Z/]*(?=/[12][0-9])|[a-zA-Z-]*[^w](?=[.]usatoday[.]com)')
wapo_url_label_re = re.compile('(?<=https://www.washingtonpost.com/)([a-z-]*/){1,}(?=wp)')
nydailynews_url_label_re = re.compile('(?<=http://www.nydailynews.com/).*')


def get_url_label(url):
    if 'nytimes.com' in url:
        try:
            return '/'.join(nytimes_url_label_re.search(url).group().split('/')[:-1])
        except:
            pass
        
    if 'usatoday' in url:
        try:
            return usatoday_url_label_re.findall(url)
        except:
            pass

    if 'theonion' in url:
        return url.replace('www.','').split('//')[-1].split('.')[0]

    if 'metro' in url:
        try:
            return '/'.join(url.replace('//','').split('/')[1:-1])
        except:
            pass

    if 'washingtonpost' in url:
        try:
            return wapo_url_label_re.search(url).group().strip('amphtml/')

        except:
            pass

    if 'nydailynews' in url:
        try:
            return  '/'.join(nydailynews_url_label_re.group().split('/')[:-1])
        except:
            pass
 
    return 'NOLABEL'


fox_label_re = re.compile('(?<=<meta name="prism.section" content=")[^"]*(?=">)')
wsj_label_re = re.compile('(?<=<meta name="article.type" content=").*(?=" />)')
nypost_label_re = re.compile('(?<="section":")[a-zA-Z0-9]*(?=")|(?<="articleSection":")[a-zA-Z0-9]*(?=")')


def get_html_label(url,html):
    if 'foxnews' in url:
        try:
            return fox_label_re.search(html).group()
        except:
            pass

    if 'wsj' in url:
        try:
            return '/'.join(wsj_label_re.search(html).group().split(' - '))
        except:
            pass

    if 'nypost' in url:
        try:
            return nypost_label_re.search(html).group()
        except:
            pass

    return 'NOLABEL'


def get_label(url,html):
    for item in 'nydailynews,nytimes,usatoday,theonion,metro,washingtonpost'.split(','):
        if item in url:
            return get_url_label(url)
            break
    
    for item in 'nypost,foxnews,wsj'.split(','):
        if item in url:
            return get_html_label(url,html)
            break


    soup = BeautifulSoup(html,'html.parser')
    soup_label = [item['content'] for item in soup.findAll('meta',content=True,property=True) if item['property'] == 'article:section' or item['property'] =='og:section']
    if len(label) != 0:
        return label[0]
    else:
        return 'NOLABEL'


if __name__ == "__main__":
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

    print("running...")
    #print("Counting number of entries...")
    datafiles = [DATAPATH + file for file in os.listdir(DATAPATH)]
    #print("Number of entries: %i" % (count_lines_many(datafiles)))


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
            try:
                text_chunks = bunch_paragraphs(filter_paragraphs(FN,paragraphs),target_length=250)
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
            except:
                pass
    file.close()
    print()
    print("%s processes. %i lines written to %s." % (DATAPATH,i,DESTFILE)) 

    with open('extraction_log.txt','a+') as file:
        file.append("%s processes. %i lines written to %s." % (DATAPATH,i,DESTFILE) + '\n')

