"""
Script for summarizing datasets
"""

import sys
import os
from collections import Counter
from multiprocessing import Pool
import json

def count_one(filename):
    global classes, total_count, sane_count
    with open(filename,'r') as file:
        for line in file:
            item = json.loads(line)
            total_count += 1
            if item[0] != 'NOLABEL' and item[1] != 'NOCONTENT':
                sane_count += 1
                print("total: %i sane: %i label: %s" % (total_count,sane_count,item[0]))
                classes.update([item[0]])

def count_many(filenames):
    p = Pool()
    p.map(count_one,filenames)
    p.close()

def write_report():
    global classes
    with open('%s_summary.txt' % FOLDER.strip('/').split('/')[-1],'w') as file:
        for item in sorted(list(classes.items()),key=lambda x: x[1]):
            file.write(json.dumps(item))



if __name__ == "__main__":
    for FOLDER in sys.argv[1:]:
        print("Generating summary for %s" % FOLDER)

        filenames = [FOLDER + file for file in os.listdir(FOLDER) if 'json' in file]
        classes = Counter()
        total_count = 0
        sane_count = 0

        count_many(filenames)
        write_report()
