package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.OrderCount;
import com.alibaba.middleware.race.jstorm.bolt.OrderDispatch;
import com.alibaba.middleware.race.jstorm.bolt.OrderParse;
import com.alibaba.middleware.race.jstorm.bolt.OrderSumWrite;
import com.alibaba.middleware.race.jstorm.bolt.PayCount;
import com.alibaba.middleware.race.jstorm.bolt.PayParseAndDispatch;
import com.alibaba.middleware.race.jstorm.bolt.PayRatioWrite;
import com.alibaba.middleware.race.jstorm.spout.TopicsSubscriberSpout;
import com.alibaba.middleware.race.util.StormRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.
 * RaceTopology； 所以这个主类路径一定要正确
 */
public class RaceTopology {

//	private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;

	public RaceTopology() {
		builder = new TopologyBuilder();
		topologyName = RaceConfig.JstormTopologyName;
		topologyConfig = createTopologyConfiguration();

		buildTopology();
	}

	private Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(4);
		return conf;
	}

	private void buildTopology() {
		int spout_Parallelism_hint = 1;
		int pay_parse_Parallelism_hint = 4;
		int order_parse_Parallelism_hint = 4;
		int converge_Parallelism_hint = 1;
		int count_Parallelism_hint = 1;

//		LOG.trace("start build");

		builder.setSpout("spout", new TopicsSubscriberSpout(), spout_Parallelism_hint);
		// note:bolt-pay-parse,bolt-tb-parse,bolt-tm-parse could have more than
		// one Parallelism hint
		// bolt-tb-count,bolt-tb-write,bolt-tm-count,bolt-tm-write should be one
		// Parallelism hint at this version
		// branch of pay
		builder.setBolt("bolt-pay-parse", new PayParseAndDispatch(), pay_parse_Parallelism_hint).shuffleGrouping("spout", "stream-pay");
		builder.setBolt("bolt-pay-pc-count", new PayCount("pc"),1).shuffleGrouping("bolt-pay-parse", "stream-pay-pc");
		builder.setBolt("bolt-pay-wl-count", new PayCount("wl"),1).shuffleGrouping("bolt-pay-parse", "stream-pay-wl");
		builder.setBolt("bolt-payratio-write", new PayRatioWrite(RaceConfig.prex_ratio),1)
				.shuffleGrouping("bolt-pay-pc-count").shuffleGrouping("bolt-pay-wl-count");

		
		builder.setBolt("bolt-order-parse", new OrderParse(), order_parse_Parallelism_hint).shuffleGrouping("spout","stream-tb").shuffleGrouping("spout","stream-tm");
		builder.setBolt("bolt-converge", new OrderDispatch(), converge_Parallelism_hint).allGrouping("bolt-order-parse")
															 .shuffleGrouping("bolt-pay-parse","stream-pay-pc").shuffleGrouping("bolt-pay-parse","stream-pay-wl");
	
		// branch of tb
		builder.setBolt("bolt-tb-count", new OrderCount(), count_Parallelism_hint).shuffleGrouping("bolt-converge","stream-tb-pay");
		builder.setBolt("bolt-tb-write", new OrderSumWrite(RaceConfig.prex_taobao), 1).shuffleGrouping("bolt-tb-count");

		// branch of tm
		builder.setBolt("bolt-tm-count", new OrderCount(), count_Parallelism_hint).shuffleGrouping("bolt-converge","stream-tm-pay");
		builder.setBolt("bolt-tm-write", new OrderSumWrite(RaceConfig.prex_tmall), 1).shuffleGrouping("bolt-tm-count");

	}

	public void startLocal() throws InterruptedException {
		// LocalCluster cluster = new LocalCluster();
		// cluster.submitTopology(topologyName, topologyConfig,
		// builder.createTopology());
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, 10);
	}

	public void startRemote() throws AlreadyAliveException, InvalidTopologyException {
		StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
	}

	public static void main(String[] args) throws Exception {
//		new RaceTopology().startLocal();
		new RaceTopology().startRemote();

	}
}