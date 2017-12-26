package com.alibaba.middleware.race.util;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.rocketmq.client.MQHelper;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class MQConsumerFactory {
	
	private static final Logger	LOG   = Logger.getLogger(MQConsumerFactory.class);
    
    private static final long serialVersionUID = 4641537253577312163L;
    
    public static Map<String, DefaultMQPushConsumer> consumers = 
    		new HashMap<String, DefaultMQPushConsumer>();
    
    public static synchronized DefaultMQPushConsumer mkInstance(MQClientConfig config, 
			MessageListenerConcurrently listener)  throws Exception{
    	
    	String[] topics = config.getTopics();
    	String groupId = config.getConsumerGroup();
    	
    	String key = topics + "@" + groupId;
    	
    	DefaultMQPushConsumer consumer = consumers.get(key);
    	if (consumer != null) {
    		
    		LOG.info("Consumer of " + key + " has been created, don't recreate it ");
    		
    		//Attention, this place return null to info duplicated consumer
    		return null;
    	}
    	
        
        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init meta client \n");
        sb.append(",configuration:").append(config);
        
        LOG.info(sb.toString());
        
        consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        
        String nameServer = config.getNameServer();
        if ( nameServer != null) {
			String namekey = "rocketmq.namesrv.domain";

			String value = System.getProperty(namekey);
			// this is for alipay
			if (value == null) {

				System.setProperty(namekey, nameServer);
			} else if (value.equals(nameServer) == false) {
				throw new Exception(
						"Different nameserver address in the same worker "
								+ value + ":" + nameServer);

			}
		}
        //tijiao daima shi shanchu
//        consumer.setNamesrvAddr(nameServer);
        //
        String instanceName = groupId +"@" +	JStormUtils.process_pid();
		consumer.setInstanceName(instanceName);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		//modify at 2016-06-28
		//subscribe multiple-topic 
		for(String topic:topics){
			consumer.subscribe(topic, config.getSubExpress());
		}
		//------------------------------------------------------
		consumer.registerMessageListener(listener);
		
		consumer.setPullThresholdForQueue(config.getQueueSize());
		consumer.setConsumeMessageBatchMaxSize(config.getSendBatchSize());
		consumer.setPullBatchSize(config.getPullBatchSize());
		consumer.setPullInterval(config.getPullInterval());
		consumer.setConsumeThreadMin(config.getPullThreadNum());
		consumer.setConsumeThreadMax(config.getPullThreadNum());


		Date date = config.getStartTimeStamp() ;
		if ( date != null) {
			LOG.info("Begin to reset meta offset to " + date);
			try {
				for (String topic : topics) {
					MQHelper.resetOffsetByTimestamp(
							MessageModel.CLUSTERING
							, instanceName
							, config.getConsumerGroup()
							, topic
							, date.getTime());
				}
				LOG.info("Successfully reset meta offset to " + date);
			}catch(Exception e) {
				LOG.error("Failed to reset meta offset to " + date);
			}

		}else {
			LOG.info("Don't reset meta offset  ");
		}

		consumer.start();
		
		consumers.put(key, consumer);
		LOG.info("Successfully create " + key + " consumer");
		
		
		return consumer;
		
    }
    
}
