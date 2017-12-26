package com.alibaba.middleware.race.jstorm.bolt;

public class PayCacheWrapper {


	@Override
	public String toString() {
		return "PayCacheWrapper [orderId=" + orderId + ", time_ms=" + time_ms + ", amount=" + amount + ", count="
				+ count + "]";
	}

	private Long orderId;
	private Long time_ms;
	private Double amount;
	
	//this wrapper will be counted after get from the cache
	private int count;
	
	public PayCacheWrapper(Long orderId, Long time_ms, Double amount){
		this(orderId,time_ms,amount,0);
	}
	
	public PayCacheWrapper(Long orderId, Long time_ms, Double amount, int count){
		this.orderId = orderId;
		this.time_ms = time_ms;
		this.amount = amount;
		this.count = count;
	}


	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}


	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}

	public Long getTime_ms() {
		return time_ms;
	}

	public void setTime_ms(Long time_ms) {
		this.time_ms = time_ms;
	}

	public Long getOrderId() {
		return orderId;
	}

	public void setOrderId(Long orderId) {
		this.orderId = orderId;
	}

}
