package com.vteba.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yinlei
 * @since 2018/6/26 9:43
 */
public class LearnHBase {

    public static final Logger LOGGER = LoggerFactory.getLogger(LearnHBase.class);

    public static final void main(String[] args) {
        // @deprecated
//        HConnection
//        HTablePool
//        HTablePool tablePool = new HTablePool();
//        HTableInterface testTable = tablePool.getTable("test1");
//        try {
////            Get get = new Get();
//            testTable.close();
//        } catch (IOException e) {
//            LOGGER.info("error=[{}]", e.getMessage());
//        }

        try {
            Configuration configuration = new Configuration();
            Connection connection = ConnectionFactory.createConnection(configuration);

        } catch (Exception e) {

        }
    }
}
