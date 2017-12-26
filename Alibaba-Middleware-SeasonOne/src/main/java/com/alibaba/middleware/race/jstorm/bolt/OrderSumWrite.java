package com.alibaba.middleware.race.jstorm.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleUtils;

public class OrderSumWrite extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3908892649829991262L;

//	private static final Logger LOG = LoggerFactory.getLogger(OrderSumWrite.class);

	private TairOperatorImpl writer;

	private String prex;

	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 1;

	private final int emitFrequencyInSeconds;

	private Map<String, HashMap<Long, Double>> receive = new HashMap<String, HashMap<Long, Double>>();
	
	private Map<Long, Double> result;

	public OrderSumWrite(String prex) {
		this.prex = prex;
		this.emitFrequencyInSeconds = DEFAULT_EMIT_FREQUENCY_IN_SECONDS;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		writer = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
				RaceConfig.TairGroup, RaceConfig.TairNamespace);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		if (TupleUtils.isTick(input)) {
			this.result = sumUp(receive);
			for(Entry<Long, Double> tmp : result.entrySet()){
				write2Tair(tmp);
			}
//			LOG.info("orders count:" + result);
		} else {
			try {
				MapWrapper<Long, Double> map = (MapWrapper<Long, Double>) input.getValue(1);
				String threadName = input.getString(0);
				receive.put(threadName, map.getContainer());

			} catch (Exception e) {
//				LOG.error(Thread.currentThread().getName() + e.getMessage());
			}
		}
//		LOG.info(Thread.currentThread().getName() + input);
	}

	private Map<Long, Double> sumUp(Map<String, HashMap<Long, Double>> receive){
		Map<Long, Double> result_tmp = new HashMap<Long, Double>();
		for(Entry<String, HashMap<Long, Double>> entry : receive.entrySet()){
			for(Entry<Long, Double> tmp : entry.getValue().entrySet()){
				
				if (result_tmp.containsKey(tmp.getKey())) {
					Double value_amount = tmp.getValue() + result_tmp.get(tmp.getKey());
					result_tmp.put(tmp.getKey(), value_amount);
				} else {
					result_tmp.put(tmp.getKey(), tmp.getValue());
				}
				
			}
		}
		
		return result_tmp;
	}
	


	private void write2Tair(Entry<Long, Double> entry) {
		String key = prex + entry.getKey();

		// confirm the entry in tair
		Double value_get = 0.0;
		try {
			Result<DataEntry> rs = writer.get(key);
			if (rs != null) {
				value_get = (Double) rs.getValue().getValue();
			}

		} catch (Exception e) {

		}

		if (!entry.getValue().equals(value_get)) {
			// write the key-value until done
			while (!writer.write(key, entry.getValue()))
				;
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}

}
