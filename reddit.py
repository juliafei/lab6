import sys
import csv
from pyspark import SparkContext
from collections import defaultdict

def main(sc):
    COMMENTS = "/data/share/reddit/RC_2006_01.bz2"
    rdd = sc.textFile(COMMENTS, use_unicode=False).cache()
    filename = 'redditresults.txt'
    
    comments = rdd.take(1000)

    columns = defaultdict(list) # each value in each column is appended to a list

    with open('asthmaQualifications.csv') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            for (i,v) in enumerate(row):
                columns[i].append(v)
    print(columns[0])
    appnames = (columns[0])
    
    comments = [c.lower() for c in comments]
    appnames = [a.lower() for a in appnames]

    output = [c for c in comments if any(a in c for a in appnames)]
    
    output.saveAsTextFile(filename)

    
if __name__ == "__main__":
     sc = SparkContext()
     # Execute Main functionality
     main(sc)

