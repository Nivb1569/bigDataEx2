import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-word-count-ex2")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("hdfs://" + sys.argv[1])
    word_counts_rdd = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
    word_counts_rdd.cache()
    total_distinct_words = word_counts_rdd.count()
    print("--------------------------------------------")
    print(f"Total distinct words found: {total_distinct_words}")

    filtered_counts = word_counts_rdd.filter(lambda x: len(x[0])>5)
    result_list = filtered_counts.takeOrdered(40, key = lambda x: -x[1])

    # print (repr(list)[1:-1])
    print(*result_list, sep="\n")
    print("--------------------------------------------")
