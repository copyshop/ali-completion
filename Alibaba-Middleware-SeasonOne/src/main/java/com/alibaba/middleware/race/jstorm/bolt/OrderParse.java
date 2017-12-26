package com.alibaba.middleware.race.jstorm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OrderParse extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3114751226166625890L;

//	private static final Logger LOG = LoggerFactory.getLogger(OrderParse.class);

    private OutputCollector collector;
    
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        
    }

	public void execute(Tuple input) {
		OrderMessage orderMessage = null;
		byte[] body = new byte[]{};

//		count++;
//		Log.info("--------" + Thread.currentThread().getName() + prex + count);
		try {
			body = ((byte[])input.getValue(1));
			orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
			
//			Long minuteTime = (orderMessage.getCreateTime() / 1000 / 60) * 60;
//			String key = prex + minuteTime;
			collector.emit(new Values(input.getValue(0),orderMessage.getOrderId()));
		} catch (Exception e) {
			if(body.length == 2 && body[0] == 0 && body[1] == 0){
//				LOG.info("---------------Got the end signal----------------");
				//try to use this signal
			}
//			LOG.error("" + e.getMessage());
		}
		
		

	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic","orderId"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
