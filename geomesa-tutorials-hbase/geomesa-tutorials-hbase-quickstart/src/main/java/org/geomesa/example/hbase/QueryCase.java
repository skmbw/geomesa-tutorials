package org.geomesa.example.hbase;

import org.geotools.data.*;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yinlei
 * @since 2018/7/3 15:20
 */
public class QueryCase implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryCase.class);

    private DataStore dataStore;
    private Query query;

    public QueryCase(Query query) {
        this.dataStore = DataStoreUtils.getInstance().getDataStore();
        this.query = query;
    }

    public void run() {
        String queryString = ECQL.toCQL(query.getFilter());
        try {
            LOGGER.debug("Running query " + queryString);

            long d = System.currentTimeMillis();
            FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                    dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
            int n = 0;
            for (; reader.hasNext(); ) {
                reader.next();
                n++;
            }
            long time = System.currentTimeMillis() - d;
            LOGGER.info("Returned [{}] total features, 用时time=[{}]毫秒", n, time);
        } catch (Exception e) {
            LOGGER.error("查询错误queryString=[{}]", queryString, e);
        }
    }
}
