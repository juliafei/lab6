import sys
import pyproj
from pyspark import SparkContext

def main(sc):
    
    taxi = sc.textFile('yellow.csv.gz')
    bike = sc.textFile('citibike.csv')
    gLoc=(40.73901691,-74.00263761)
    
    def filterBike(pId, lines):
        import csv
        for row in csv.reader(lines):
            if(row[6] == 'Greenwich Ave & 8 Ave' and 
                row[3].startswith('2015-02-01')):
                yield (row[3][:19])

    gBike = bike.mapPartitionsWithIndex(filterBike).cache()
    gBike.take(5)
    
    def filterTaxi(pId, lines):
        if pId == 0:
            next(lines)
        import csv
        for row in csv.reader(lines):
            droppoff = row[4], row[5]
            yield row[1]

    gTaxil= taxi.mapPartitionsWithIndex(filterTaxi).cache()
    gTaxil.take(5)
    
    
    gAll = (gTaxil.map(lambda x: (x, 0)) +
           gBike.map(lambda x: (x, 1)))
    gAll.sortByKey().take(5)
    
    df =sqlContext.createDataFrame(gAll, ('time', 'event'))
    dfl = df.select(df['time'].cast('timestamp').cast('long').alias('epoch'), 'event')
    dfl.registerTempTable('gAll')
    
    #window function sql
    query = '''
    SELECT sum(has_taxi) FROM
    (
        SELECT event, 1-min(event) OVER (ORDER BY epoch 
                        RANGE BETWEEN 600  PRECEDING 
                            AND CURRENT ROW)
            AS  has_taxi
        FROM gAll

    ) newGALL
    WHERE event = 1
    '''

    sqlContext.sql(query).write.csv('lab10.csv')
    
if __name__ == "__main__":
     sc = SparkContext()
     # Execute Main functionality
     main(sc)

