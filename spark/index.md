# Spark


1. 导⼊pyspark.sql，创建spark session

   ```SPARQL
   from pyspark import SparkConf
   from pyspark.sql import SparkSession
   import pyspark.sql.functions as f
   from pyspark.sql.functions import array_contains
   spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
   ```

   

2. 读取 yelp_academic_dataset_business.json ⽂件，创建DataFrame

   ```SPARQL
   chushi = spark.read.json('file:///usr/local/src/yelp_academic_dataset_business.json')
   ```

   

3. 分割数据集中 categories ，剔除数据集中缺少城市信息的数据

   ```SPARQL
   import pyspark.sql.functions as f
   chushitype = f.split(chushi["categories"],',')
   fenge = chushi.withColumn("categories",chushitype)
   chuli = fenge.dropna(subset=['city'])
   ```

4. 显示数据集数据50条

   ```SPARQL
   chuli.show(50)
   ```


5. 找出 Las Vegas(城市) 中所有 Burgers（汉堡店，类型） （20分），将结果按照星级降序排序，显示前50条 结果

   ```SPARQL
   cityLs = chuli.filter(chuli["city"] == "Las Vegas")
   hb = cityLs.filter(array_contains(cityLs["categories"],"Burgers"))
   
   jieg = hb.orderBy(-hb["stars"]).show(50)
   ```

6. 找出所有全美商业种类总数（10分），排序输出数量前⼗的商业种类（10分）

   ```SPARQL
   quchu = chuli.filter(chuli["latitude"]>25).filter(chuli["latitude"]<49).filter(chuli["longitude"]<-70).filter(chuli["longitude"]>-130)
   quchu.select(quchu["categories"]).distinct().count()
   
   lps = chushi.select(chushi["categories"]).rdd.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).toDF()
   lps.orderBy(-lps["_2"]).show(10)
   ```

   

   


