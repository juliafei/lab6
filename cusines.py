import sys
from pyspark import SparkContext

def main(sc):
    
    FOOD = '/data/share/bdm/nyc_restaurants.csv'
    food = sc.textFile(FOOD, use_unicode=False).cache()
    list(enumerate(food.first().split(',')))

    def getCuisine(partId, list_of_rest):
        count = 0
        if partId == 0:
            list_of_rest.next()
        import csv
        reader = csv.reader(list_of_rest)
        for row in reader:
            if row[14]!= 'F':
                (cuisine) = (row[7])
                count = count + 1
                yield (cuisine)

    cuisine = food.mapPartitionsWithIndex(getCuisine)

    cc = cuisine.flatMap(lambda line: line.split()) \
            .map(lambda x: (x.lower(), 1)) \
            .groupByKey() \
            .mapValues(lambda values: sum(values))
    cc.top(25, key=lambda x: x[1])
    
if __name__ == "__main__":
     sc = SparkContext()
     # Execute Main functionality
     main(sc)

