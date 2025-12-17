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


Exercise 2:

Q1: 

```
import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input_folder>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("python-word-count-ex2")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile("hdfs://" + sys.argv[1])
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
```

Q2:

77928

<img width="1366" height="768" alt="image" src="https://github.com/user-attachments/assets/e8e8748d-45fa-46f2-ad53-e3121326d793" />

Exercise 3:

<img width="1366" height="653" alt="image" src="https://github.com/user-attachments/assets/17669b62-4728-4572-ba71-70c3a66f0012" />

Exercise 4:

Q1:

```
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
```

Q2:

<img width="1366" height="768" alt="image" src="https://github.com/user-attachments/assets/465653bc-e71b-4147-99af-a75427c5da7c" />

Exercise 5:

Q1:

```
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
```

Q2:

<img width="1366" height="768" alt="image" src="https://github.com/user-attachments/assets/b6f51896-ab67-4004-9df7-01066d937a44" />





