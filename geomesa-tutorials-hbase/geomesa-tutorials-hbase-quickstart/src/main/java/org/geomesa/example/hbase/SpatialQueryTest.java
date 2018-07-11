package org.geomesa.example.hbase;

import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yinlei
 * @since 2018/7/3 15:14
 */
public class SpatialQueryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SpatialQueryTest.class);

    public static void main(String[] args) {
        String bbox = "bbox(geom,-75,41,-73,39)";
        try {
            long d = System.currentTimeMillis();
            Query query = new Query("gdelt-quickstart", ECQL.toFilter(bbox));
            QueryCase queryCase = new QueryCase(query);
            int count = 240;
            for(int i = 0; i < count; i++) {
                queryCase.run();
            }
            long time = System.currentTimeMillis() - d;
            LOGGER.info("查询次数count=[{}], 用时time=[{}]毫秒, 每次查询响应时间=[{}]毫秒.", count, time, time / count);
        } catch (Exception e) {
            LOGGER.error("空间查询错误。", e);
        }
    }
}
