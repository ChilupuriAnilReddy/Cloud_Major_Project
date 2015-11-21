from flask import Flask
import sys
from flask import request
from operator import add
from json import JSONDecoder
from json import JSONEncoder
from flask import jsonify
from pyspark import SparkContext
import matplotlib.pyplot as plt
app = Flask(__name__)



@app.route('/')
def hello_world():
    return 'Hello World!'



@app.route('/toptenmovies')
def top_ten_movies():
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
    toprint={}
    for (word, count) in output:
        toprint[i] = ans[int(word)]
        if i==10:
            break
        i = i  + 1;
    sc.stop()
    return jsonify(toprint) 


@app.route('/ratinggraph',methods=['GET'])
def ratinggraph():
    movieid = int(request.args.get('movieid'))
    print movieid
    print "xx"
    sc = SparkContext(appName="graph") 
    lines = sc.textFile(sys.argv[1], 1)
    line2 = lines.filter(lambda line: "movieId" not in line)    
    line1 = line2.filter(lambda line: int(line.split(',')[1])==movieid)
    
    counts = line1.map(lambda x: (x.split(',')[3], (x.split(',')[2]))) \
                  .reduceByKey(add) \
                  .sortBy(lambda x: x[0])

    output1 = counts.collect()
       
    print "Timestamp\tRating"
    i = 1;
    xx=[]
    yy=[]
    for (word, count) in output1:
        xx.append(float(i))
        yy.append(float(count))
        if i==100:
            break
        i = i + 1;
        print "%s\t%s" % (word, count)

    fig = plt.figure(figsize=(10,10))
    plt.plot(xx,yy)  
    plt.show()
    sc.stop()
    return 'Done!'

    
     

if __name__ == '__main__':
    app.run(debug=True)
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: Rating <file>"
        exit(-1)
