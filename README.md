# bigDataEx2

Name: Niv Baumel.

Id: 322755737

Exercise 1:

Q1: 

```
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-word-count")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("hdfs://" + sys.argv[1])
    counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b) \
             .repartition(5) \
             .filter(lambda x: len(x[0])>5)
    # "takeOrdered" is an action. 
    list = counts.takeOrdered(40, key = lambda x: -x[1])
    print("--------------------------------------------")
    # print (repr(list)[1:-1])
    print(*list, sep="\n")
    print(counts.take(5))
    print("--------------------------------------------")
```


Q2: 

<img width="1366" height="652" alt="image" src="https://github.com/user-attachments/assets/4ff1c407-09f9-430a-a13e-085eb63c65db" />

    print(*list, sep="\n")
    print(counts.take(5))
    print("--------------------------------------------")
