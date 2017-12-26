package com.alibaba.middleware.race.jstorm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleUtils;

public abstract class Count extends BaseRichBolt {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = -8735655575875616646L;

//	private static final Logger LOG = LoggerFactory.getLogger(OrderCount.class);

    protected OutputCollector collector;
    
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 10;
    
    protected final int emitFrequencyInSeconds;
    
//    protected int count;
    
    protected HashMap<Long,Double> container = new HashMap<Long,Double>();
    
    public Count(){
    	this(DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }
    
    public Count(int emitFrequencyInSeconds){
    	this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    }
    
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	public void execute(Tuple input) {
		if (TupleUtils.isTick(input)) {
			emit();
		}else{
			try {
//				String key = "" + (input.getLong(2)/1000/60)*60;
//				Double value = input.getDouble(3);
//				++count;
				Long key = getKey(input);
				Double value = getValue(input);
				if (container.containsKey(key)) {
					value = value + container.get(key);
					container.put(key, value);
				} else {
					container.put(key, value);
				}
			} catch (Exception e) {
//				LOG.error(Thread.currentThread().getName() + e.getMessage());
//				e.printStackTrace();
			}
//			System.out.println("container:" + container);
		}
//		LOG.info(Thread.currentThread().getName() + "receive msg:" + count);
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public abstract void declareOutputFields(OutputFieldsDeclarer declarer);

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
	    return conf;
	}
	
	protected abstract void emit();
	
	protected abstract Long getKey(Tuple input);
	
	protected abstract Double getValue(Tuple input);

}
