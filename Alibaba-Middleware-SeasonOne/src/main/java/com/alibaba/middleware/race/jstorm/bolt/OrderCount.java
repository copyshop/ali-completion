package com.alibaba.middleware.race.jstorm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OrderCount extends Count {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7949216173920472674L;
	
	private String thread;
	
	public OrderCount(){
		super(5);	
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.thread = Thread.currentThread().getName();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("threadname","result"));
		
	}

	@Override
	protected void emit() {
		this.collector.emit(new Values(thread,new MapWrapper(this.container)));
		
	}

	@Override
	protected Long getKey(Tuple input) {
		return (input.getLong(0)/1000/60)*60;
	}

	@Override
	protected Double getValue(Tuple input) {
		return input.getDouble(1);
	}




}
