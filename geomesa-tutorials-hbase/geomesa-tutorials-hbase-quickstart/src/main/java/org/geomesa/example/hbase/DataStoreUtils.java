package org.geomesa.example.hbase;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yinlei
 * @since 2018/7/3 15:33
 */
public class DataStoreUtils {

    private static final DataStoreUtils INS = new DataStoreUtils();
    private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreUtils.class);

    private DataStore dataStore;

    private DataStoreUtils() {
        // 命令行时，参数前面要带--
        Map<String, String> params = new HashMap<>();
        params.put("hbase.zookeepers", "10.0.12.145");
        params.put("hbase.catalog", "t1");
        try {
            dataStore = DataStoreFinder.getDataStore(params);
        } catch (IOException e) {
            LOGGER.error("创建DataStore错误。", e);
        }
    }

    public static DataStoreUtils getInstance() {
        return INS;
    }

    public DataStore getDataStore() {
        return dataStore;
    }
}
