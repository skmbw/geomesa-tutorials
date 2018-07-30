package org.geomesa.example.hbase;

import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author yinlei
 * @since 2018/7/3 15:15
 */
public class TemporalQueryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemporalQueryTest.class);

    public static void main(String[] args) {
        String during = "dtg DURING 2015-12-31T00:00:00.000Z/2016-01-02T00:00:00.000Z";
        // 启动线程数
        int threads = 8;
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(threads);
            long totalTime = 0;
//            long d = System.currentTimeMillis();
            Query query = new Query("gdelt-quickstart", ECQL.toFilter(during));
            QueryCase queryCase = new QueryCase(query);
            int count = 2500;
            List<Future<Long>> list = new ArrayList<>();
            for(int i = 0; i < count; i++) {
//                queryCase.run();
                Future<Long> future = executorService.submit(queryCase);
                list.add(future);
            }

            for (Future<Long> future : list) {
                Long usedTime = future.get();
                totalTime += usedTime;
                LOGGER.info("temporal query task is done = [{}]!", future.isDone());
            }

//            long time = System.currentTimeMillis() - d;
//            LOGGER.info("查询次数count=[{}], 用时time=[{}]毫秒, 每次查询响应时间=[{}]毫秒.", count, time, time / count);
            LOGGER.info("查询次数count=[{}], 任务用时time=[{}]毫秒, 每次查询响应时间=[{}]毫秒.", count, totalTime, totalTime / count);

            executorService.shutdown();
        } catch (Exception e) {
            LOGGER.error("时间查询错误。", e);
        }
    }
}
