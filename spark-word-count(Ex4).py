import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: longestword <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("longest-word-ex4")
    sc = SparkContext(conf=conf)
    text_file = sc.textFile(sys.argv[1])

    def clean_word(word):
        return word.replace(".", "").replace(",", "")

    longest_word = text_file.flatMap(lambda line: line.split(" ")) \
        .map(clean_word) \
        .filter(lambda word: len(word) > 0 and word.isalpha()) \
        .reduce(lambda w1, w2: w1 if len(w1) > len(w2) else w2)

    print("--------------------------------------------")
    print(f"The longest word found is: {longest_word}")
    print(f"Length: {len(longest_word)}")
    print("--------------------------------------------")
