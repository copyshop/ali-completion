package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Producer，发送消息
 */
public class Producer {

	private static Random rand = new Random();
	private static int count = 4000000;

	private static Map<String, Double> tbContainer = new LinkedHashMap<String, Double>();

	private static Map<String, Double> tmContainer = new LinkedHashMap<String, Double>();

	private static Map<String, Double> pcContainer = new LinkedHashMap<String, Double>();

	private static Map<String, Double> wlContainer = new LinkedHashMap<String, Double>();
	
	private static Map<String, Double> ratio = new LinkedHashMap<String, Double>();
	
	private static int count_tb = 0;
	private static int count_tm = 0;
	private static int count_pay = 0;

	public static void sumTbTm(OrderMessage orderMessage, int platform) {
		if (platform == 0) {
			count_tb++;
			String key = RaceConfig.prex_taobao + (orderMessage.getCreateTime() / 1000 / 60) * 60;
			Double value = orderMessage.getTotalPrice();
			if (tbContainer.containsKey(key)) {
				value = tbContainer.get(key) + value;
				tbContainer.put(key, value);
			} else {
				tbContainer.put(key, value);
			}
		} else {
			count_tm++;
			String key = RaceConfig.prex_tmall + (orderMessage.getCreateTime() / 1000 / 60) * 60;
			Double value = orderMessage.getTotalPrice();
			if (tmContainer.containsKey(key)) {
				value = tmContainer.get(key) + value;
				tmContainer.put(key, value);
			} else {
				tmContainer.put(key, value);
			}
		}
	}
	
	public static void sumPay(PaymentMessage paymentMessage){
		count_pay++;
		String key = ""+(paymentMessage.getCreateTime()/1000/60)*60;
		Double value = paymentMessage.getPayAmount();
		if(paymentMessage.getPayPlatform() == 0){
			
			if(pcContainer.containsKey(key)){
				value = pcContainer.get(key) + value;
				pcContainer.put(key, value);
			}else{
				pcContainer.put(key, value);
			}
		}else{

			if(wlContainer.containsKey(key)){
				value = wlContainer.get(key) + value;
				wlContainer.put(key, value);
			}else{
				wlContainer.put(key, value);
			}
		}
	}
	
	public static void calcRatio(){
		if(pcContainer.size() == wlContainer.size()){
			
			Set<Entry<String, Double>> pcSet = pcContainer.entrySet();
			
			for(Entry<String, Double> entry : pcSet){
				ratio.put(entry.getKey(), wlContainer.get(entry.getKey())/entry.getValue());
			}
		}
	}
	

	/**
	 * 这是一个模拟堆积消息的程序，生成的消息模型和我们比赛的消息模型是一样的， 所以选手可以利用这个程序生成数据，做线下的测试。
	 * 
	 * @param args
	 * @throws MQClientException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws MQClientException, InterruptedException {
		DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

		// 在本地搭建好broker后,记得指定nameServer的地址
		producer.setNamesrvAddr("127.0.0.1:9876");

		producer.start();

		final String[] topics = new String[] { RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic };
		final Semaphore semaphore = new Semaphore(0);

		for (int i = 0; i < count; i++) {
			try {
				final int platform = rand.nextInt(2);
				final OrderMessage orderMessage = (platform == 0 ? OrderMessage.createTbaoMessage()
						: OrderMessage.createTmallMessage());
				orderMessage.setCreateTime(System.currentTimeMillis());
				//add 2016-07-01
				sumTbTm(orderMessage,platform);
				
				byte[] body = RaceUtils.writeKryoObject(orderMessage);

				Message msgToBroker = new Message(topics[platform], body);

				producer.send(msgToBroker, new SendCallback() {
					public void onSuccess(SendResult sendResult) {
//						System.out.println(orderMessage);
						semaphore.release();
					}

					public void onException(Throwable throwable) {
						throwable.printStackTrace();
					}
				});

				// Send Pay message
				PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
				double amount = 0;
				for (final PaymentMessage paymentMessage : paymentMessages) {
					int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
					if (retVal < 0) {
						throw new RuntimeException("price < 0 !!!!!!!!");
					}

					if (retVal > 0) {
						//add 2016-07-01
						sumPay(paymentMessage);
						
						amount += paymentMessage.getPayAmount();
						final Message messageToBroker = new Message(RaceConfig.MqPayTopic,
								RaceUtils.writeKryoObject(paymentMessage));
						producer.send(messageToBroker, new SendCallback() {
							public void onSuccess(SendResult sendResult) {
//								System.out.println(paymentMessage);
							}

							public void onException(Throwable throwable) {
								throwable.printStackTrace();
							}
						});
					} else {
						//
					}
				}

				if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
					throw new RuntimeException("totalprice is not equal.");
				}

			} catch (Exception e) {
				e.printStackTrace();
				Thread.sleep(1000);
			}
		}

		semaphore.acquire(count);

		// 用一个short标识生产者停止生产数据
		byte[] zero = new byte[] { 0, 0 };
		Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
		Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
		Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);

		try {
			producer.send(endMsgTB);
			producer.send(endMsgTM);
			producer.send(endMsgPay);
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.shutdown();
		
		calcRatio();
		System.out.println(tbContainer);
		System.out.println(tmContainer);
		System.out.println(pcContainer);
		System.out.println(wlContainer);
		System.out.println(ratio);
		System.out.println("tb:"+count_tb);
		System.out.println("tm:"+count_tm);
		System.out.println("pay:"+count_pay);

	}
}
