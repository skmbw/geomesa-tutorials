package org.geomesa.example.hbase;

import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author yinlei
 * @since 2018/6/20 14:11
 */
public class GeoMesaInsertDataToHBase {

    public static final Logger LOGGER = LoggerFactory.getLogger(GeoMesaInsertDataToHBase.class);

    public static void main(String[] args) throws IOException {
        try {
            String[] params = {"--hbase.zookeepers", "10.0.12.145", "--hbase.catalog", "t2"};
            GeoMesaTask task = new GeoMesaTask(params, new HBaseDataStoreFactory().getParametersInfo());
            task.run();
        } catch (Exception e) {
            LOGGER.error("启动geomesa test异常。error=[{}]", e);
        }
    }

}
