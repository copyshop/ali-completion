package com.alibaba.middleware.race.jstorm.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class PayRatioWrite extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4244299617761137624L;

//	private static final Logger LOG = LoggerFactory.getLogger(PayRatioWrite.class);

	private String prex;

	private TairOperatorImpl writer;

	private Map<Long, Double> pcContainer;

	private Map<Long, Double> wlContainer;

	private Map<String, Double> ratio = new HashMap<String, Double>();

	public PayRatioWrite(String prex) {
		this.prex = prex;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		writer = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
				RaceConfig.TairGroup, RaceConfig.TairNamespace);

	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		// System.out.println(Thread.currentThread().getName() + ": " + input);

		try {
			MapWrapper<Long, Double> map = (MapWrapper<Long, Double>) input.getValue(1);
			if (input.getValue(0).equals("pc")) {
				pcContainer = map.getContainer();
			} else {
				wlContainer = map.getContainer();
			}
		} catch (Exception e) {
//			LOG.error(Thread.currentThread().getName() + e.getMessage());
		}

		if (pcContainer != null && wlContainer != null && pcContainer.size() == wlContainer.size()) {

			ArrayList<Map.Entry<Long, Double>> pcList = this.sort(pcContainer.entrySet());
			ArrayList<Map.Entry<Long, Double>> wlList = this.sort(wlContainer.entrySet());
//			LOG.info("pcList:" + pcList);
			Double curPcAmount = 0.0;
			Double curWlAmount = 0.0;
			if(pcList != null && wlList != null && pcList.size() == wlList.size()){
				for(int i = 0; i < pcList.size(); i++){
					curPcAmount = curPcAmount + pcList.get(i).getValue();
					curWlAmount = curWlAmount + wlList.get(i).getValue();
					
					String key = prex + pcList.get(i).getKey();
					Double value = curWlAmount/curPcAmount;
					ratio.put(key, value);
					
					
					write2Tair(key,value);
		
				}
			}
		}

		// System.out.println(Thread.currentThread().getName() + ": " + ratio);
//		LOG.info(Thread.currentThread().getName() + "ratio history:  " + ratio);

	}
	
	private void write2Tair(String key,Double value){
		//confirm the entry in tair
		Double value_get = 0.0;
		try {
			Result<DataEntry> rs = writer.get(key);
			if(rs != null){
				value_get = (Double)rs.getValue().getValue();
			}
			
		} catch (Exception e) {
			
		}
		
		if (!value.equals(value_get)) {
			// write the key-value until done
			while (!writer.write(key, value));
		}
	}
	
	private ArrayList<Map.Entry<Long, Double>> sort(Set<Entry<Long, Double>> entrySet){
		ArrayList<Map.Entry<Long, Double>> tmpList = new ArrayList<Map.Entry<Long, Double>>(entrySet);
		Collections.sort(tmpList, new Comparator<Map.Entry<Long, Double>>(){

			@Override
			public int compare(Entry<Long, Double> o1, Entry<Long, Double> o2) {
				return o1.getKey() > o2.getKey() ? 1 : -1;
			}			
		});
		return tmpList;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
