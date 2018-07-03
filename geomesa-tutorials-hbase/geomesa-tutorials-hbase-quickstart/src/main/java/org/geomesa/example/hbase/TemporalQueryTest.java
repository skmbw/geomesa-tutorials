package org.geomesa.example.hbase;

import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yinlei
 * @since 2018/7/3 15:15
 */
public class TemporalQueryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemporalQueryTest.class);

    public static void main(String[] args) {
        String during = "dtg DURING 2015-12-31T00:00:00.000Z/2016-01-02T00:00:00.000Z";
        try {
            long d = System.currentTimeMillis();
            Query query = new Query("gdelt-quickstart", ECQL.toFilter(during));
            QueryCase queryCase = new QueryCase(query);
            int count = 10000;
            for(int i = 0; i < count; i++) {
                queryCase.run();
            }
            long time = System.currentTimeMillis() - d;
            LOGGER.info("查询次数count=[{}], 用时time=[{}], 每次查询响应时间=[{}]毫秒.", count, time, time / count);
        } catch (Exception e) {
            LOGGER.error("空间查询错误。", e);
        }
    }
}
