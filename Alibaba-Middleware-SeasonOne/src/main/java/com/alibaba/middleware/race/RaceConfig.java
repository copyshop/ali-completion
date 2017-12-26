package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
	
	private static String teamCode = "37071jju1t";
    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_" + teamCode + "_";
    public static String prex_taobao = "platformTaobao_" + teamCode + "_";
    public static String prex_ratio = "ratio_" + teamCode + "_";


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "37071jju1t";
    public static String MetaConsumerGroup = "37071jju1t";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 38383;
	
	
//    //这些是写tair key的前缀
//    public static String prex_tmall = "platformTmall_";
//    public static String prex_taobao = "platformTaobao_";
//    public static String prex_ratio = "ratio_";
//
//
//    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
//    public static String JstormTopologyName = "testConsole";
//    public static String MetaConsumerGroup = "xxx";
//    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
//    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
//    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
//    public static String TairConfigServer = "localhost:5198";
//    public static String TairSalveConfigServer = "xxx";
//    public static String TairGroup = "group_1";
//    public static Integer TairNamespace = 0;
}
