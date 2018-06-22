/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.hbase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.geomesa.example.data.SimpleGDELTData;
import org.geomesa.example.data.TutorialData;
import org.geomesa.example.quickstart.CommandLineDataStore;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.*;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class GeoMesaTask implements Runnable {

    private final Map<String, String> params;
    private final boolean readOnly;

    public static final Logger LOGGER = LoggerFactory.getLogger(GeoMesaTask.class);

    public GeoMesaTask(String[] args, Param[] parameters) throws ParseException {
        this(args, parameters, false);
    }

    public GeoMesaTask(String[] args, Param[] parameters, boolean readOnly) throws ParseException {
        // parse the data store parameters from the command line
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        this.readOnly = readOnly;
    }

    public Options createOptions(Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);
        if (!readOnly) {
            options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());
        }
        return options;
    }

    @Override
    public void run() {
        File directory = new File("D:\\downloads\\doc\\GDELT\\2016");
        File[] fileList = directory.listFiles();
        if (fileList == null) {
            LOGGER.info("没有获取到GDELT文件。");
            return;
        }
        // date parser corresponding to the CSV format
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US);

        TutorialData gdeltData = new SimpleGDELTData();

        InputStream is = null;
        DataStore datastore;

        try {
            datastore = createDataStore(params);

            SimpleFeatureType sft = getSimpleFeatureType(gdeltData);
            createSchema(datastore, sft);

            for (File file : fileList) {
                LOGGER.info("处理文件：[" + file.getPath() + "]开始...");
                SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
                is = decompressZip(file);
                CSVParser parser = CSVParser.parse(is, StandardCharsets.UTF_8, CSVFormat.TDF);

                List<SimpleFeature> featureList = new ArrayList<>();
                for (CSVRecord record : parser) {
                    try {
                        // 获取数据，设置和SimpleFeature相应的属性
                        builder.set("GLOBALEVENTID", record.get(0));
                        // some dates are converted implicitly, so we can set them as strings
                        // however, the date format here isn't one that is converted, so we parse it into a java.util.Date

                        builder.set("dtg",
                                Date.from(LocalDate.parse(record.get(1), dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant()));

//                        builder.set("Actor1Name", record.get(6));
//                        builder.set("Actor1CountryCode", record.get(7));
//                        builder.set("Actor2Name", record.get(16));
//                        builder.set("Actor2CountryCode", record.get(17));
                        builder.set("EventCode", record.get(26));

                        // we can also explicitly convert to the appropriate type
//                        builder.set("NumMentions", Integer.valueOf(record.get(31)));
//                        builder.set("NumSources", Integer.valueOf(record.get(32)));
//                        builder.set("NumArticles", Integer.valueOf(record.get(33)));
//
//                        builder.set("ActionGeo_Type", record.get(51));
//                        builder.set("ActionGeo_FullName", record.get(50));
//                        builder.set("ActionGeo_CountryCode", record.get(52));

                        // we can use WKT (well-known-text) to represent geometries
                        // note that we use longitude first ordering
                        double[] latlng = getLatAndLng(record);
                        // POINT(经度，维度)
                        builder.set("geom", "POINT (" + latlng[1] + " " + latlng[0] + ")");

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

                LOGGER.info("解析文件[" + file.getPath() + "]构建feature完成。");
                writeFeatures(datastore, sft, featureList);

                LOGGER.info("处理文件：[" + file.getPath() + "]结束.");
            }
        } catch (Exception e) {
            // 精度和维度越界[-180,180][-90,90]
            LOGGER.error(e.getMessage());
        }

        IOUtils.closeQuietly(is);
    }

    /**
     * 解压zip文件，返回流
     *
     * @param originFile zip文件。
     * @return 如果目标文件不是zip文件抛异常
     */
    public static InputStream decompressZip(File originFile) {
        InputStream is = null;
        try {
            ZipFile zipFile = new ZipFile(originFile);
            ZipEntry zipEntry;
            Enumeration<? extends ZipEntry> entry = zipFile.entries();
            while (entry.hasMoreElements()) { // 这个处理多个，其实我们的业务就一个
                zipEntry = entry.nextElement();
                is = zipFile.getInputStream(zipEntry);
            }
        } catch (Exception e) {
            LOGGER.error("解压zip文件错误。" + e.getMessage());
        }
        return is;
    }

    public static double[] getLatAndLng(CSVRecord record) {
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
            latitude = getLat();
            longitude = getLng();
            LOGGER.debug("没有获取到经纬度,模拟的经纬度是lat=[" + latitude + "], lng=[" + longitude + "]");
        } else {
            LOGGER.debug("经纬度是lat=[" + lat + "], lng=[" + lng + "]");
            latitude = Double.parseDouble(lat);
            longitude = Double.parseDouble(lng);
        }
        return new double[]{latitude, longitude};
    }

    public static double getLatOrLng(double min, double max) {
        BigDecimal db = new BigDecimal(Math.random() * (max - min) + min);
        return db.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue(); // 小数后6位
    }

    /**
     * 维度的范围 [-90,90]
     * @return 维度
     */
    public static double getLat() {
        return getLatOrLng(3, 53);
    }

    /**
     * 经度的范围 [-180,180]
     * @return 经度
     */
    public static double getLng() {
        return getLatOrLng(73, 135);
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        LOGGER.info("开始创建 geomesa hbase datastore.");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            LOGGER.info("创建 geomesa hbase datastore 异常。");
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        LOGGER.info("创建 geomesa hbase datastore 成功。");
        return datastore;
    }

    public SimpleFeatureType getSimpleFeatureType(TutorialData data) {
        return data.getSimpleFeatureType();
    }

    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        LOGGER.info("Creating schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
//        datastore.updateSchema("gdelt-quickstart", sft);
        datastore.createSchema(sft);
    }

    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() > 0) {
            LOGGER.info("开始 Writing GDELT data，一共 Wrote " + features.size() + " 条 features");
            // use try-with-resources to ensure the writer is closed
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                     datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation class
                    // and add the USE_PROVIDED_FID hint to the user data
                    ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // make sure to copy the user data, if there is any
                    toWrite.getUserData().putAll(feature.getUserData());

                    // write the feature
                    writer.write();
                }
            }
//            LOGGER.info("一共 Wrote " + features.size() + " 条 features");
        }
    }

}
