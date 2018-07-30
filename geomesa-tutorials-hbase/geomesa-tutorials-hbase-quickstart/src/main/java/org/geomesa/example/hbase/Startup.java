package org.geomesa.example.hbase;

/**
 * @author yinlei
 * @since 2018/6/12 14:46
 */
public class Startup {

    public static void main(String[] args) throws Exception {
         String[] params = {"--hbase.zookeepers", "localhost", "--hbase.catalog", "test1", "cleanup"};
//         String[] params = {"--hbase.zookeepers", "10.0.12.145", "--hbase.catalog", "test1", "cleanup"};
//        String[] params = {"--hbase.connection", "hdfs://localhost:9000/hbase", "--hbase.catalog", "test1"};
        HBaseQuickStart quickStart = new HBaseQuickStart(params);
        quickStart.run();
    }
}
