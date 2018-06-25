package org.geomesa.example.hbase;

import org.geomesa.example.data.SimpleGDELTData;
import org.geomesa.example.data.TutorialData;
import org.geotools.data.*;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.sort.SortBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yinlei
 * @since 2018/6/22 17:30
 */
public class QueryTest {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryTest.class);

    public static void main(String[] args) {
        Map<String, String> params = new HashMap<>();
        // 命令行时，参数前面要带--
        params.put("hbase.zookeepers", "10.0.12.145");
        params.put("hbase.catalog", "t1");

        try {
            DataStore datastore = DataStoreFinder.getDataStore(params);

            TutorialData data = new SimpleGDELTData();

            // 使用这个查询，数据里面有的没有数据，时间范围不在这里
            List<Query> queryList = data.getTestQueries();

            for (Query query : queryList) {
                LOGGER.info("Running query " + ECQL.toCQL(query.getFilter()));
                if (query.getPropertyNames() != null) {
                    LOGGER.info("Returning attributes " + Arrays.asList(query.getPropertyNames()));
                }
                if (query.getSortBy() != null) {
                    SortBy sort = query.getSortBy()[0];
                    LOGGER.info("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
                }
                // submit the query, and get back an iterator over matching features
                // use try-with-resources to ensure the reader is closed
                try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                             datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                    // loop through all results, only print out the first 10
                    int n = 0;
                    for (;reader.hasNext();) {
                        SimpleFeature feature = reader.next();
                        if (n++ < 10) {
                            // use geotools data utilities to get a printable string
                            LOGGER.info(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                        } else if (n == 10) {
                            LOGGER.info("更多...");
                        }
                    }
                    LOGGER.info("Returned " + n + " total features");
                }
            }
        } catch (Exception e) {
            LOGGER.error("创建DataStore ERROR=[{}]", e);
        }
    }
}
