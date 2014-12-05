import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount <file>"
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    #tmp = 'total'
    num = 0
    for (word, count) in output:
        #if word == 'total':
	#print "%s: %i" % (word, count)
	#if (word is tmp):
		num = num + count 

    print "%i" % (num)

