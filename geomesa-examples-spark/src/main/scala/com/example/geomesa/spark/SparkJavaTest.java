package com.example.geomesa.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.geotools.data.Query;
import org.locationtech.geomesa.spark.GeoMesaSpark;
import org.locationtech.geomesa.spark.SpatialRDD;
import org.locationtech.geomesa.spark.hbase.HBaseSpatialRDDProvider;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yinlei
 * @since 2018/9/19 14:25
 */
public class SparkJavaTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://10.0.12.145:7077").setAppName("appName");

        SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);

        Map<String, Serializable> params = new HashMap<>();
        HBaseSpatialRDDProvider rddProvider = (HBaseSpatialRDDProvider) GeoMesaSpark.apply(params);
        // 和params是相同的
        scala.collection.immutable.Map<String, String> immap = new scala.collection.immutable.HashMap<>();
        SpatialRDD spatialRDD = rddProvider.rdd(new Configuration(), sparkContext, immap, new Query());

    }
}
