from pyspark import SparkConf, SparkContext


def wordcount():
    # 设置spark应用程序的运行模式和应用在webui的名称
    conf = SparkConf().setMaster("local").setAppName("wordcount")
    # sc是通往spark集群的唯一通道
    sc = SparkContext(conf=conf)

    # 读取文件
    lines = sc.textFile("../data/words")
    # 根据空格对一行文本进行分割
    words = lines.flatMap(lambda line: line.split(" "))
    # 对每个单词计数,并转化为元组
    pair_words = words.map(lambda word: (word, 1))
    # 根据相同的key的value值相加
    reduce_words = pair_words.reduceByKey(lambda v1, v2: v1 + v2)
    # 根据value的值进行排序
    sorted_words = reduce_words.sortBy(lambda t: t[1], False)
    # 执行action算子，打印结果
    sorted_words.foreach(lambda t: print(t))


def main():
    wordcount()


if __name__ == '__main__':
    main()
