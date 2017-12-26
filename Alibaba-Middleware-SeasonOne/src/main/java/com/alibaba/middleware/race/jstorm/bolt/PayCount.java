package com.alibaba.middleware.race.jstorm.bolt;


import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class PayCount extends Count {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8815883599852210309L;
	
	private String platform;
	
	public PayCount(String platform){
		super();
		this.platform = platform;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("platform","map"));		
	}

	@Override
	protected void emit() {
		this.collector.emit(new Values(platform , new MapWrapper(this.container)));
	}

	@Override
	protected Long getKey(Tuple input) {
		return (input.getLong(2)/1000/60)*60;
	}

	@Override
	protected Double getValue(Tuple input) {
		return input.getDouble(3);
	}
    
}
