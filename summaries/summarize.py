"""
Script for summarizing datasets
"""

import sys
import os
from collections import Counter
from multiprocessing import Pool
import json

def count_one(filename):
    global total_count, sane_count
    classes = Counter()
    with open(filename,'r') as file:
        for line in file:
            item = json.loads(line)
            total_count += 1
            if item[0] != 'NOLABEL' and item[1] != 'NOCONTENT':
                sane_count += 1
                if sane_count % 1000 == 0:
                    print("total: %i sane: %i label: %s classes: %i" % (total_count,sane_count,item[0],len(classes)))
                classes.update([item[0]])
    return classes

def count_many(filenames):
    p = Pool()
    classes_ = p.map(count_one,filenames)
    p.close()
    return classes_

if __name__ == "__main__":
    if len(sys.argv[1:]) != 0:
        folders=  sys.argv[1:]
    else: 
        folders = ['/datapool/news_articles/' + foldername for foldername in os.listdir('/datapool/news_articles/') if 'raw_data' not in foldername]
    for FOLDER in folders:
        print("Generating summary for %s" % FOLDER)
        FOLDER = '/'+FOLDER.strip('/') + '/'
        filenames = [FOLDER + filename for filename in os.listdir(FOLDER) if 'json' in filename]
        total_count = 0
        sane_count = 0

        classes_ = count_many(filenames)

        from functools import reduce
        classes = reduce(lambda x,y: x+y, classes_)
        
        print("Number of classes: %i" % len(classes))
        log = open('%s_summary.txt' % FOLDER.strip('/').split('/')[-1],'w+')
        log.write("Total classes: %i\n" % len(classes))
        log.write("Total entries: %i\n" % sum([item[1] for item in classes.items()]))
        for item in sorted(list(classes.items()),key = lambda x: -x[1]):
            itemstr = json.dumps(item)
            print("Item: %s" % itemstr)
            log.write(itemstr+'\n')
        log.flush()
        log.close()
        print("Summary written")
