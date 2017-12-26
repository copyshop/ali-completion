package com.alibaba.middleware.race.jstorm.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleUtils;

public class OrderDispatch extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5104562069395522767L;

//	private static final Logger LOG = LoggerFactory.getLogger(OrderCount.class);

	protected OutputCollector collector;

	private Random rand = new Random();

	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 10;

	private final int emitFrequencyInSeconds;

//	private HashMap<Long, String> orderIdMap = new HashMap<Long, String>();
	
	private HashSet<Long> TbOrderMap = new HashSet<Long>();
	
	private HashSet<Long> TmOrderMap = new HashSet<Long>();

	private LinkedList<PayCacheWrapper> payInfo = new LinkedList<PayCacheWrapper>();

	//for debug
	private int count_pay;

	private int count_order;

	private int count_emit;

	private int count_emit_later;

	public OrderDispatch() {
		this(DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
	}

	public OrderDispatch(int emitFrequencyInSeconds) {
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if (TupleUtils.isTick(input)) {
			emit();
		} else if (input.getString(0).equals(RaceConfig.MqTaobaoTradeTopic)) {
			++count_order;
			this.TbOrderMap.add(input.getLong(1));
		} else if (input.getString(0).equals(RaceConfig.MqTmallTradeTopic)) {
			++count_order;
			this.TmOrderMap.add(input.getLong(1));
		}else {
			++count_pay;

			if (TbOrderMap.contains(input.getLong(1))) {
				this.collector.emit("stream-tb-pay", new Values(input.getLong(2), input.getDouble(3)));
				++count_emit;
			} else if (TmOrderMap.contains(input.getLong(1))) {
				this.collector.emit("stream-tm-pay", new Values(input.getLong(2), input.getDouble(3)));
				++count_emit;
			} else {
				this.payInfo.add(new PayCacheWrapper(input.getLong(1), input.getLong(2), input.getDouble(3)));
				
				if(this.payInfo.size() > 1000 && this.payInfo.size() < 2000){
					emit();
				}
			}
		}
//		LOG.info(Thread.currentThread().getName() + " pay:" + count_pay + " order:" + count_order + " emit:"
//				+ count_emit + "emit_later" + count_emit_later + "payInfo size:" + payInfo.size());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("stream-tb-pay", new Fields("time_ms", "amount"));
		declarer.declareStream("stream-tm-pay", new Fields("time_ms", "amount"));
	}

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}

	private void emit() {

		Iterator<PayCacheWrapper> itr = payInfo.iterator();
		while (itr.hasNext()) {
			PayCacheWrapper entry = itr.next();
			if (TbOrderMap.contains(entry.getOrderId())) {
				this.collector.emit("stream-tb-pay", new Values(entry.getTime_ms(), entry.getAmount()));
				itr.remove();
				++count_emit_later;
			} else if (TmOrderMap.contains(entry.getOrderId())) {
				this.collector.emit("stream-tm-pay", new Values(entry.getTime_ms(), entry.getAmount()));
				itr.remove();
				++count_emit_later;
			}
		}
	}

}
