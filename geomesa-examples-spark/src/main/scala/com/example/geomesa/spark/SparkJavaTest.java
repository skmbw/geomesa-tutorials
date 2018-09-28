package com.example.geomesa.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.spark.GeoMesaSpark;
import org.locationtech.geomesa.spark.SpatialRDD;
import org.locationtech.geomesa.spark.hbase.HBaseSpatialRDDProvider;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yinlei
 * @since 2018/9/19 14:25
 */
public class SparkJavaTest {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://10.0.12.145:7077").setAppName("appName");

        SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);

        Map<String, Serializable> params = new HashMap<>();
        params.put("user", "root");
        params.put("password", "123456");
        params.put("hbase.zookeepers", "server1");
        params.put("hbase.zookeeper.quorum", "server1,server2,server3");
        params.put("hbase.catalog", "t1");
        params.put("tableName", "t1");
        HBaseSpatialRDDProvider rddProvider = (HBaseSpatialRDDProvider) GeoMesaSpark.apply(params);

        String typeName = "gdelt-quickstart";
        String geom = "geom";
        String date = "dtg";

        String bbox = "-80, 35, -79, 36";
        String during = "2016-01-01T00:00:00.000Z/2016-01-31T12:00:00.000Z";

        String filter = "bbox(" + geom + ", " + bbox + ") AND " + date + " during " + during;

        // 和params是相同的
        scala.collection.immutable.Map<String, String> map = new scala.collection.immutable.HashMap<>();
        map = map.$plus(new Tuple2<>("password", "123456"));
        map = map.$plus(new Tuple2<>("user", "root"));
        map = map.$plus(new Tuple2<>("hbase.zookeepers", "server1"));
        map = map.$plus(new Tuple2<>("hbase.zookeeper.quorum", "server1,server2,server3"));
        map = map.$plus(new Tuple2<>("hbase.catalog", "t1"));
        map = map.$plus(new Tuple2<>("tableName", "t1"));
        Query query = new Query(typeName, ECQL.toFilter(filter));
        SpatialRDD spatialRDD = rddProvider.rdd(new Configuration(), sparkContext, map, query);

    }
}
