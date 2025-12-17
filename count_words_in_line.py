import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: linecount <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("count_words_in_line")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile(sys.argv[1])

    def get_line_with_more_words(line1, line2):
        count1 = len(line1.split())
        count2 = len(line2.split())
        return line1 if count1 > count2 else line2

    busiest_line = text_file.filter(lambda line: len(line.strip()) > 0) \
                            .reduce(get_line_with_more_words)

    word_count = len(busiest_line.split())

    print("--------------------------------------------")
    print(f"Found the line with the most words ({word_count} words):")
    print(busiest_line)
    print("--------------------------------------------")
