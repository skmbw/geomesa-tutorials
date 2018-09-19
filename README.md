GeoMesa Tutorials and Examples
==============================

See the official GeoMesa [documentation](http://www.geomesa.org/documentation/tutorials/index.html) for instructions.

## 运行spark
* 本地运行
spark-submit --master local[*] --class com.example.geomesa.spark.CountByDay geomesa-examples-spark-2.1.0-SNAPSHOT.jar
* 集群运行
spark-submit --master yarn --class com.example.geomesa.spark.CountByDay geomesa-examples-spark-2.1.0-SNAPSHOT.jar

## 本地的spark版本和远程的spark版本不一致的话，会导致序列化异常，可能是本机引用的spark版本不同，更改就好了。也可能是pom文件中冲突
