package com.alibaba.middleware.race.jstorm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PayParseAndDispatch extends BaseRichBolt {
    
	/**
	 * 
	 */
	private static final long serialVersionUID = -2801586320120129618L;

//	private static final Logger LOG = LoggerFactory.getLogger(PayParseAndDispatch.class);

    private OutputCollector collector;

    int count_pay = 0;
    
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        
    }

	@Override
	public void execute(Tuple input) {
		byte[] body = new byte[]{};
//		System.out.println("--------"+Thread.currentThread().getName() + " length:"+body.length);
		PaymentMessage paymentMessage = null;

//		count_pay++;
//		LOG.info("--------Pay:" + Thread.currentThread().getName() + count_pay);
		try {
			body = ((byte[])input.getValue(1));
			paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
//			String key = "" + (paymentMessage.getCreateTime()/1000/60)*60;
			long key = paymentMessage.getCreateTime();
			Double value = paymentMessage.getPayAmount();
			if(paymentMessage.getPayPlatform() == 0){				
				collector.emit("stream-pay-pc",new Values(input.getValue(0),paymentMessage.getOrderId(),key,value));
			}else{
				collector.emit("stream-pay-wl",new Values(input.getValue(0),paymentMessage.getOrderId(),key,value));
			}
		} catch (Exception e) {
			if(body.length == 2 && body[0] == 0 && body[1] == 0){
//				LOG.info("---------------Got the end signal----------------");
				//try to use this signal
			}
//			LOG.error("" + e.getMessage());
		}
      

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("stream-pay-pc",new Fields("topic","orderId","time","amount"));
		declarer.declareStream("stream-pay-wl",new Fields("topic","orderId","time","amount"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
