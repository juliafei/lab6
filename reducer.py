#!/usr/bin/env python
import itertools, operator, sys

def parsePairs():
    for line in sys.stdin:
        yield tuple(line.strip('\n').split('\t'))

def reducer():
    cc = cuisine.flatMap(lambda line: line.split()) \
        .map(lambda x: (x.lower(), 1)) \
        .groupByKey() \
        .mapValues(lambda values: sum(values))
cc.top(25, key=lambda x: x[1])

if __name__=='__main__':
    reducer()