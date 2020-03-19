from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("UvApp")
sc = SparkContext(conf=conf)

# 读取数据
lines = sc.textFile("../../data/pvuvdata")

# 针对得到每一行，拆分数据，并转化为二元组
separator = "\t"
ip_websites = lines.map(lambda line: f"{line.split(separator)[0]}_{line.split(separator)[5]}")

# 去重|转换成KV|统计|排序|取top5
top5 = ip_websites.distinct().map(lambda iw: (iw.split("_")[1],1)).reduceByKey(lambda v1,v2: v1+v2).sortBy(lambda elem: elem[1], False).take(5)

print("top 5 PV website:")
for i in range(len(top5)):
    print(top5[i])
