package com.alibaba.middleware.race.jstorm.bolt;

import java.io.Serializable;
import java.util.HashMap;

public class MapWrapper<Key,Value> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5006292030892960472L;
	
	private HashMap<Key,Value> container;
	
	public MapWrapper(){
		
	}
	
	public MapWrapper(HashMap<Key,Value> container){
		this.container = container;
	}

	public HashMap<Key,Value> getContainer() {
		return container;
	}

	public void setContainer(HashMap<Key,Value> container) {
		this.container = container;
	}

	@Override
	public String toString() {
		return "MapWrapper [container=" + container + "]";
	}
}
