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
    for FOLDER in sys.argv[1:]:
        print("Generating summary for %s" % FOLDER)

        filenames = [FOLDER + file for file in os.listdir(FOLDER) if 'json' in file]
        total_count = 0
        sane_count = 0

        classes_ = count_many(filenames)
        from functools import reduce
        classes = reduce(lambda x,y: x+y, classes_)
        print("Number of classes: %i" % len(classes))
        with open('%s_summary.txt' % FOLDER,'w') as file:
            file.write("Total classes: %i\n" % len(classes))
            for item in sorted(list(classes.items()),key = lambda x: -x[1]):
                file.write(json.dumps(item)+'\n')

        print(sorted(list(classes.items()),key = lambda x: -x[1]))
        print("Summary written")
        del classes
