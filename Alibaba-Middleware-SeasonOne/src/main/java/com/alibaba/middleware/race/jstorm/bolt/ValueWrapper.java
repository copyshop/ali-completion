package com.alibaba.middleware.race.jstorm.bolt;

public class ValueWrapper {

	private Double value;
	
	private boolean isWrite = false;

	public ValueWrapper(Double value){
		this.value = value;
	}
	
	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public boolean isWrite() {
		return isWrite;
	}

	public void setWrite(boolean isWrite) {
		this.isWrite = isWrite;
	}

	@Override
	public String toString() {
		return "ValueWrapper [value=" + value + ", isWrite=" + isWrite + "]";
	}
}
