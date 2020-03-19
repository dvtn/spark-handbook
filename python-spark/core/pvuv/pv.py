from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PvApp")
sc = SparkContext(conf=conf)

# 读取数据
lines = sc.textFile("../../data/pvuvdata")

# 针对得到每一行，拆分数据，并转化为二元组
pair_websites = lines.map(lambda line: (line.split("\t")[5], 1))

# 对上面的K,V格式的累加求和
websites = pair_websites.reduceByKey(lambda v1, v2: v1 + v2)

# 排序
sorted_websites = websites.sortBy(lambda t: t[1], False)

# 打印结果
sorted_websites.foreach(lambda t: print(t))

print("top 5 PV website:")
top5 = sorted_websites.take(5)
for i in range(len(top5)):
    print(top5[i])
