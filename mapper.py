#!/usr/bin/env python
import sys, time

def parseRecords():
    for row in sys.stdin:
        row = line.strip(',')
        yield row.split()

def mapper():
    for row in parseRecords():
        for r in row:
            if row[14]!= 'F':
            (cuisine) = (row[7])
            yield (cuisine)
            

if __name__=='__main__':
    mapper()

  