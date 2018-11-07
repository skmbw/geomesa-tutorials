#!/usr/bin/env python
#coding:utf-8
"""
/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
geomesa_quickstart_jnius.py

Note: this has been tested with python 2.7.12 & 3.5

Description:
   This script is a python adaptation of the GeoMesa Accumulo quickstart. It is a query only
   demo, in contrast to the java quickstart that creates the table & loads data into it. This
   script uses two second party packages (jnius & bokeh) that need to be loaded to run. 'jnius'
   is a python-java bridge that allows python programs to run java code.
   
   On unix-based systems these packages can be installed using pip:
            1) sudo -H pip install pyjnius
            2) sudo -H pip install bokeh
   
   There are two command line options:

Created by: Jordan Muss
                   CCRi

Creation Date: 3-31-2017
Version:       1.0

Dependencies: 
         Public:     argparse, bokeh, datetime, jnius, os, re, string, sys, __future__ (print_function)
         Private:   utils.geomesa_jnius_setup, utils.quickstart_command_line_parser (calls utils.geomesa_command_line_parser)
                        tools.ECQL.datastore, tools.ECQL.filter

How to use:
Updates:

To Do: Get queryFeaturesToDict from tools/ECQL/queryFormatters
"""
from __future__ import print_function
from datetime import datetime
'''----------------------------------------------------------------------------------------------------------------------'''
''' Import Java & GeoMesa classes: '''
'''----------------------------------------------------------------------------------------------------------------------'''
from utils.geomesa_jnius_setup import *
from utils.quickstart_command_line_parser import getArgs
from pyJavaClasses.datastore import getDataStore, createAccumuloDBConf
from tools.ECQL import filter
import os
'''----------------------------------------------------------------------------------------------------------------------'''
''' Setup quickstart data access & display functions: '''
'''----------------------------------------------------------------------------------------------------------------------'''
def queryFeaturesToDict(ecql, simpleFeatureTypeName, dataStore, filter_string):
    ''' Return the results of a ECQL filter query as a dict for additional processing: '''
    ''' Submit the query, which will return a features object: '''
    features = ecql.getFeatures(simpleFeatureTypeName, dataStore, filter_string)
    ''' Get an iterator of the matching features: '''
    featureIter = features.features()
    
    ''' Loop through all results and put them into a dictionary for secondary processing: '''
    n = 0
    results = {}
    while featureIter.hasNext():
        feature = featureIter.next()
        n += 1
        results[n] = {
            "GlobalEventID": feature.getProperty("GlobalEventID").getValue(),
            "Actor1Code": feature.getProperty("Actor1Code").getValue(),
            "Actor1Name": feature.getProperty("Actor1Name").getValue()
        }
        # results[n] = {"who":feature.getProperty("Who").getValue(),
        #                       "what":feature.getProperty("What").getValue(),
        #                       "when":datetime.strptime(feature.getProperty("When").getValue().toString(), "%a %b %d %H:%M:%S %Z %Y"),
        #                       "geometry":feature.getProperty("Where").getValue(),
        #                       "where":feature.getProperty("Where").getValue().toString(),
        #                       "x":feature.getProperty("Where").getValue().x,
        #                       "y":feature.getProperty("Where").getValue().y,
        #                       "why":feature.getProperty("Why").getValue().__str__() }
    featureIter.close()
    return(results)

def printQuickStart(quickStart_dict):
    """ Print the quickstart feature dict: """
    for k in sorted(quickStart_dict.keys()):
        # print("{}.\t{}|{}|{}|{}|{}".format(k, quickStart_dict[k]["who"], quickStart_dict[k]["what"],
        #           quickStart_dict[k]["when"].strftime("%a %b %d %H:%M:%S %Z %Y"),
        #           quickStart_dict[k]["where"], quickStart_dict[k]["why"]))
        print("{}.\t{}|{}".format(k, quickStart_dict[k]["GlobalEventID"], quickStart_dict[k]["Actor1Code"],
                                           quickStart_dict[k]["Actor1Name"]))
'''---------------------------------------------------------------------------------------------------------------'''
''' End GeoMesa query functions: '''
'''---------------------------------------------------------------------------------------------------------------'''

if __name__ == "__main__":
    '''--------------------------------------------------------------------------------------------------------------'''
    ''' Get the runtime options: '''
    '''--------------------------------------------------------------------------------------------------------------'''
    # 这个可以去掉
    # args = getArgs()
    '''--------------------------------------------------------------------------------------------------------------'''
    ''' Setup jnius for GeoMesa java calls: '''
    '''--------------------------------------------------------------------------------------------------------------'''
    # classpath = args.classpath

    # 加载当前目录lib下的jar
    classpath = os.path.dirname(os.path.abspath(__file__)) + '/lib/'
    # jni 就是jnius的对象，有了他就可以autoclass java对象
    jni = SetupJnius(classpath=classpath)
    '''--------------------------------------------------------------------------------------------------------------'''
    ''' Setup data for GeoMesa query: '''
    '''--------------------------------------------------------------------------------------------------------------'''
    simpleFeatureTypeName = "newgdelt"
    # dsconf_dict = {'instanceId':args.instanceId,
    #                  'zookeepers':args.zookeepers,
    #                  'user':args.user,
    #                  'password':args.password,
    #                  'tableName':args.tableName }
    # 这里是 geomesa的连接参数配置
    dsconf_dict = {
        "hbase.zookeepers": "server1",
        "hbase.catalog": "gdelt2",
        "tableName": "newgdelt"
    }

    dsconf = createAccumuloDBConf(jni, dsconf_dict)

    # 获得dataStore就可以做响应的操作了，这个是底层的存储。应该使用GeoMesaDataSource，这个是封装的api
    # 应该修改getDataStore方法，返回GeoMesaDataSource对象，这样就可以使用封装的api了
    dataStore = getDataStore(jni, dsconf) # this may not work in python 3.5
    ECQL = filter.ECQLQuery(jni)

    # 这是一个范围查询的例子
    combined_filter = "bbox(Actor1Point,-79.0198,42.83,-70.9278,39.759861)"
    quickstart = queryFeaturesToDict(ECQL, simpleFeatureTypeName, dataStore, combined_filter)
    printQuickStart(quickstart)

    # if not args.no_print:
    #     # bbox_filter = filter.createBBoxFilter("Where", -77.5, -37.5, -76.5, -36.5)
    #     # when_filter = filter.createDuringFilter("When", "2014-07-01T00:00:00.000Z", "2014-09-30T23:59:59.999Z")
    #     # who_filter = filter.createAttributeFilter("(Who = 'Bierce')")
    #     # combined_filter = "{} AND {} AND {}".format(bbox_filter, when_filter, who_filter)
    #
    #     # 这是一个范围查询的例子
    #     combined_filter = "bbox(Actor1Point,-79.0198,42.83,-70.9278,39.759861)"
    #     quickstart = queryFeaturesToDict(ECQL, simpleFeatureTypeName, dataStore, combined_filter)
    #     printQuickStart(quickstart)
    
    # if args.plot:
    #     from utils.geomesa_plotting import plotGeoPoints
    #     all_pts_bbox_filter = filter.createBBoxFilter("Where", -78.0, -39.0, -76.0, -37.0)
    #     all_points = queryFeaturesToDict(ECQL, simpleFeatureTypeName, dataStore, all_pts_bbox_filter)
    #     plotGeoPoints(all_points, "Quickstart demo (jnius)", save_dir=args.out_dir)
