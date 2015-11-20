import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: Rating <file>"
        exit(-1)
    sc = SparkContext(appName="rating")    
    lines = sc.textFile(sys.argv[1], 1)
    line1 = lines.filter(lambda line: "movieId" not in line)    
    counts = line1.map(lambda x: (x.split(',')[1], float(x.split(',')[2]))) \
                  .reduceByKey(add)
    output = counts.sortBy(lambda x: -x[1]).collect()

    lines = sc.textFile(sys.argv[2], 1)
    line1 = lines.filter(lambda line: "movieId" not in line)    
    counts = line1.map(lambda x: (x.split(',')[0], (x.split(',')[1])))
    output1 = counts.collect()
    ans={}
    for (a,b) in output1:
        ans[int(a)]=b;
    i=1;
    print "MovieId\tRating\tMovieName"
    for (word, count) in output:
        print "%s\t%i\t%s" % (word, count,ans[int(word)])
        if i == 10:
            break
        i = i  + 1;

    sc.stop()
