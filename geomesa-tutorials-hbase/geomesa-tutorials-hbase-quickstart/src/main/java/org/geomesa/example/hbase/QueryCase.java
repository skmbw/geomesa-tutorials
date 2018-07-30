package org.geomesa.example.hbase;

import org.geotools.data.DataStore;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * @author yinlei
 * @since 2018/7/3 15:20
 */
public class QueryCase implements Callable<Long> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryCase.class);

    private DataStore dataStore;
    private Query query;

    public QueryCase(Query query) {
        this.dataStore = DataStoreUtils.getInstance().getDataStore();
        this.query = query;
    }

    public Long call() {
        String queryString = ECQL.toCQL(query.getFilter());
        try {
            LOGGER.debug("Running query " + queryString);

            long d = System.currentTimeMillis();
            FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                    dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
            int n = 0;
            int size = 0;
            for (; reader.hasNext(); ) {
                SimpleFeature feature = reader.next(); // 忽略结果
//                Object object = feature.getDefaultGeometry();
//                List<Object> list = feature.getAttributes();
//                FeatureType featureType = feature.getFeatureType();
                int bytes = feature.toString().getBytes().length - 19;
//                LOGGER.info("一次请求的大小[{}]", bytes);
                size += bytes;
                n++;
            }
            long time = System.currentTimeMillis() - d;
            LOGGER.info("Returned [{}] total features, 用时time=[{}]毫秒", n, time);
            LOGGER.info("Returned 数据大小= [{}] KB", size / 1024);
            return time;
        } catch (Exception e) {
            LOGGER.error("查询错误queryString=[{}]", queryString, e);
            return 0L;
        }
    }
}
