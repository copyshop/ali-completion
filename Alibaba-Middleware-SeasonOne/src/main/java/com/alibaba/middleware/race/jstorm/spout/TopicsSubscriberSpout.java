package com.alibaba.middleware.race.jstorm.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.codahale.JHistogram;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.RaceTopology;
import com.alibaba.middleware.race.util.MQClientConfig;
import com.alibaba.middleware.race.util.MQConsumerFactory;
import com.alibaba.middleware.race.util.MQTuple;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TopicsSubscriberSpout implements IRichSpout,MessageListenerConcurrently{

	private static final long serialVersionUID = 8476906628618859716L;
    
//	private static final Logger LOG = LoggerFactory.getLogger(TopicsSubscriberSpout.class);

    //configuration about the RocketMQ Client
	protected MQClientConfig mqClientConfig;
	protected transient DefaultMQPushConsumer consumer;
	
	protected SpoutOutputCollector collector;

	protected Map conf;
	protected String id;
	protected boolean flowControl;
	protected boolean autoAck;

	//stores the messages from RocketMQ
	protected transient LinkedBlockingQueue<List<MessageExt>> sendingQueue;

//	//no idea about this,leave it,figure out later
//	protected transient MetricClient metricClient;
//	protected transient AsmHistogram waithHistogram;
//	protected transient AsmHistogram processHistogram;
	
	public TopicsSubscriberSpout(){
		
	}
	
//	//no idea about this,leave it,figure out later
//	public void initMetricClient(TopologyContext context) {
//		metricClient = new MetricClient(context);
//		waithHistogram = metricClient.registerHistogram("MetaTupleWait", null);
//		processHistogram = metricClient.registerHistogram("MetaTupleProcess",null);
//	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		//init the fields
		this.conf = conf;
		this.collector = collector;
		this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
		this.sendingQueue = new LinkedBlockingQueue<List<MessageExt>>(100);

		this.flowControl = JStormUtils.parseBoolean(
				conf.get(MQClientConfig.META_SPOUT_FLOW_CONTROL), true);		
		//logs
//		StringBuilder sb = new StringBuilder();
//		sb.append("Begin to init TopicsSubscriberSpout:").append(id);
//		sb.append(", flowControl:").append(flowControl);
//		LOG.info( sb.toString());

		//subscribe three topics
		mqClientConfig = new MQClientConfig(RaceConfig.MetaConsumerGroup
											,"127.0.0.1:9876"
											,new String[]{RaceConfig.MqPayTopic,RaceConfig.MqTaobaoTradeTopic,RaceConfig.MqTmallTradeTopic}
											,"*");

		try {
			consumer = MQConsumerFactory.mkInstance(mqClientConfig, this);
		} catch (Exception e) {
//			LOG.error("Failed to create Meta Consumer ", e);
			throw new RuntimeException("Failed to create MetaConsumer" + id, e);
		}

		if (consumer == null) {
//			LOG.warn(id
//					+ " already exist consumer in current worker, don't need to fetch data ");

			new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							break;
						}

						StringBuilder sb = new StringBuilder();
						sb.append("Only on meta consumer can be run on one process,");
						sb.append(" but there are mutliple spout consumes with the same topic@groupid meta, so the second one ");
						sb.append(id).append(" do nothing ");
//						LOG.info(sb.toString());
					}
				}
			}).start();
		}
		
//		LOG.info("Successfully init " + id);
	}

	@Override
	public void close() {
		if (consumer != null) {
			consumer.shutdown();
		}

	}


	@Override
	public void activate() {
		if (consumer != null) {
			consumer.resume();
		}

	}

	@Override
	public void deactivate() {
		if (consumer != null) {
			consumer.suspend();
		}
	}

	private void sendTuple(List<MessageExt> mqTuple) {
		for(MessageExt msg : mqTuple){
			if(msg.getTopic().equals(RaceConfig.MqPayTopic)){
        		collector.emit("stream-pay", new Values(msg.getTopic(), msg.getBody()));
        	}else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
        		collector.emit("stream-tb",  new Values(msg.getTopic(), msg.getBody()));
        	}else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
           		collector.emit("stream-tm",  new Values(msg.getTopic(), msg.getBody()));
        	}
		}
	}

	@Override
	public void nextTuple() {
		List<MessageExt> mqTuple = null;
		try {
			mqTuple = sendingQueue.take();
		} catch (InterruptedException e) {
		}

		if (mqTuple == null) {
			return;
		}

		sendTuple(mqTuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("stream-pay",new Fields("topic","body"));
		declarer.declareStream("stream-tb",new Fields("topic","body"));
		declarer.declareStream("stream-tm",new Fields("topic","body"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	
	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {
		
//		LOG.info(Thread.currentThread().getName() + " consume: " + msgs.size());

		try {
			if (flowControl) {
				sendingQueue.put(msgs);
			} else {
				sendTuple(msgs);
			}

			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

		} catch (Exception e) {
//			LOG.error("Failed to emit " + id, e);
			return ConsumeConcurrentlyStatus.RECONSUME_LATER;
		}

	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}


}
