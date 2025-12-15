import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-word-count-ex3")
    sc = SparkContext(conf=conf)

    # CHANGE FOR EXERCISE 3: Using s3a:// protocol
    # The bucket name will be passed as sys.argv[1]
    text_file = sc.textFile("s3a://" + sys.argv[1])

    all_words_counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

    all_words_counts.cache()

    filtered_counts = all_words_counts.filter(lambda x: len(x[0])>5)

    list_top_40 = filtered_counts.takeOrdered(40, key = lambda x: -x[1])

    print("--------------------------------------------")
    print(*list_top_40, sep="\n")
    print("--------------------------------------------")
    print("Total distinct words (all lengths):", all_words_counts.count())
