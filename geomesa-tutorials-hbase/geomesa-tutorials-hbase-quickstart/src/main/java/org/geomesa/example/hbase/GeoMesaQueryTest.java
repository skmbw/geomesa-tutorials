package org.geomesa.example.hbase;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.geomesa.example.data.SimpleGDELTData;
import org.geotools.data.DataStore;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.geomesa.example.hbase.GeoMesaTask.decompressZip;
import static org.geomesa.example.hbase.GeoMesaTask.getLat;
import static org.geomesa.example.hbase.GeoMesaTask.getLng;

/**
 * @author yinlei
 * @since 2018/6/20 15:37
 */
public class GeoMesaQueryTest {
    public static final Logger LOGGER = LoggerFactory.getLogger(GeoMesaQueryTest.class);

    public static final void main(String[] args) {
        File directory = new File("D:\\downloads\\doc\\GDELT\\2016");
        File[] fileList = directory.listFiles();
        if (fileList == null) {
            LOGGER.info("没有获取到GDELT文件。");
            return;
        }
        // date parser corresponding to the CSV format
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US);

        SimpleGDELTData gdeltData = new SimpleGDELTData();

        InputStream is = null;
        DataStore datastore;

        try {
            for (File file : fileList) {
                LOGGER.info("处理文件：[" + file.getPath() + "]开始...");
                SimpleFeatureBuilder builder = new SimpleFeatureBuilder(gdeltData.getSimpleFeatureType());
                is = decompressZip(file);
                CSVParser parser = CSVParser.parse(is, StandardCharsets.UTF_8, CSVFormat.TDF);

                List<SimpleFeature> featureList = new ArrayList<>();
                long d = System.currentTimeMillis();
                for (CSVRecord record : parser) {
                    try {
                        // 获取数据，设置和SimpleFeature相应的属性
                        builder.set("GLOBALEVENTID", record.get(0));
                        builder.set("dtg",
                                Date.from(LocalDate.parse(record.get(1), dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant()));
                        builder.set("EventCode", record.get(26));

                        String lat = record.get(53);
                        if (StringUtils.isBlank(lat)) {
                            lat = record.get(46);
                            if (StringUtils.isBlank(lat)) {
                                lat = record.get(39);
                            }
                        }
                        String lng = record.get(54);
                        if (StringUtils.isBlank(lng)) {
                            lng = record.get(47);
                            if (StringUtils.isBlank(lng)) {
                                lng = record.get(40);
                            }
                        }

                        double latitude;
                        double longitude;
                        if (StringUtils.isAnyBlank(lat, lng)) {
                            LOGGER.debug("没有获取到经纬度，将模拟生成一个。");
                            latitude = getLat();
                            longitude = getLng();
                            LOGGER.debug("模拟的经纬度是lat=[" + latitude + "], lng=[" + longitude + "]");
                        } else {
                            LOGGER.debug("经纬度是lat=[" + lat + "], lng=[" + lng + "]");
                            latitude = Double.parseDouble(record.get(53));
                            longitude = Double.parseDouble(record.get(54));
                        }

                        builder.set("geom", "POINT (" + longitude + " " + latitude + ")");

                        // be sure to tell GeoTools explicitly that we want to use the ID we provided
                        // 明确的告诉GeoTools，我们想使用我们自己提供的ID
                        builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

                        // build the feature - this also resets the feature builder for the next entry
                        // use the GLOBALEVENTID as the feature ID
                        SimpleFeature feature = builder.buildFeature(record.get(0));
                        featureList.add(feature);
                    } catch (Exception e) {
                        LOGGER.info("Invalid GDELT record: " + e.toString() + " " + record.toString());
                    }
                }
                long time = System.currentTimeMillis() - d;
                LOGGER.info("处理文件：[" + file.getPath() + "]结束.记录数=[" + featureList.size() + "],用时time=[" + time + "]");
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

        IOUtils.closeQuietly(is);
    }

    public String randomLonLat(double MinLon, double MaxLon, double MinLat, double MaxLat, String type) {
        BigDecimal db = new BigDecimal(Math.random() * (MaxLon - MinLon) + MinLon);
        String lon = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();// 小数后6位
        db = new BigDecimal(Math.random() * (MaxLat - MinLat) + MinLat);
        String lat = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();
        if (type.equals("Lon")) {
            return lon;
        } else {
            return lat;
        }
    }
}
