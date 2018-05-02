"""
General utility functions
"""

import sys
import os
from time import time
from multiprocessing import Pool
from numpy import random
from inspect import getgeneratorstate


def count_lines_one(filename):
    """counts the number of entries in a file"""
    i = 0
    with open(filename,'r') as file:
        for line in file:
            i+=1
    return i

def count_lines_many(filenames,processes = 4):
    """count the number of lines of many files using parallel processes"""
    with Pool(4) as pool:
        line_counts = pool.map(count_lines_one,filenames)
    return sum(line_counts)


def data_from_one(filename):
    """generator that returns lines one-by-one from a file"""
    with open(filename,'r') as file:
        for line in file:
            yield line

def data_from_many(filenames,seed=42,max_files=None):
    """
    generator that returns lines from any of many files enabling random sampling.
    max_files is the maximum number of files that are being sampled from at any given time
    (too many files open at the same time is no good)

    Takes 50+ seconds for 145k lines / 15.86GB
    """
    random.seed = seed
    gens = [data_from_one(filename) for filename in filenames]
    num_gens = len(gens)
    while num_gens > 0:
        if max_files:
            x = random.randint(min(max_files,num_gens))
        else:
            x = random.randint(num_gens)
        gen = gens[x]
        yield from gen
        if getgeneratorstate(gen) == 'GEN_CLOSED':
            gens.pop(x)
            num_gens = len(gens)

def data_from_many_slow(filenames,seed=42):
    """
    generator that returns lines from any of many files enabling random sampling. 
    
    for extremely large amounts of files, a max-files hoop should be added.
    
    Takes 300+ seconds for 145k lines / 15.86 GB

    One day, find out why this is slower.
    """
    random.seed = seed
    gens = [data_from_one(filename) for filename in filenames]
    num_gens = len(gens)
    while num_gens > 0:
        x = random.randint(num_gens)
        try: 
            yield next(gens[x])
        except StopIteration:
            gens.pop(x)
            num_gens = len(gens)
