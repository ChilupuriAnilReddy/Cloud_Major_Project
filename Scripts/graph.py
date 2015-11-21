import sys
from operator import add
from pyspark import SparkContext
import matplotlib.pyplot as plt


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: Rating <file>"
        exit(-1)

    sc = SparkContext(appName="graph") 
    movieid = int(sys.argv[3])
    lines = sc.textFile(sys.argv[2], 1)
    line2 = lines.filter(lambda line: "movieId" not in line)    
    line1 = line2.filter(lambda line: int(line.split(',')[0])==movieid)
    
    counts = line1.map(lambda x: (x.split(',')[1], (x.split(',')[0])))

    output1 = counts.collect()
    
    print "MovieName\tID"
    for (word, count) in output1:
        movieid = int(count)
        print "%s\t%s" % (word, count)

    movieid=0
    lines = sc.textFile(sys.argv[1], 1)
    line2 = lines.filter(lambda line: "movieId" not in line)    
    line1 = line2.filter(lambda line: int(line.split(',')[1])==movieid)
    
    counts = line1.map(lambda x: (x.split(',')[3], (x.split(',')[2]))) \
                  .sortBy(lambda x: x[0])

    output1 = counts.collect()
    
    print "Timestamp\tRating"
    for (word, count) in output1:
        print "%s\t%s" % (word, count)


    labels = counts.map(lambda (x, y): x).collect()
    #print labels
    fracs  = counts.map(lambda (x, y): y).collect()
    #print fracs
        
    fig = plt.figure(figsize=(8,3))
    plt.plot(labels,fracs)  
    plt.show()
   
        
    sc.stop()
