package com.alibaba.middleware.race.util;

public class MessageWrapper {
	/**
	 * 消息主题
	 */
	private String topic;

	/**
	 * 消息体
	 */
	private byte[] body;

	public MessageWrapper(){
		
	}
	
	public MessageWrapper(String topic,byte[] body){
		this.topic = topic;
		this.body = body;
	}
	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}
}
