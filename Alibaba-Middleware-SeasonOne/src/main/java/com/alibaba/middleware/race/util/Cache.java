package com.alibaba.middleware.race.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache<Key, Value> - 缓存数据管理器，线程安全。支持：泛型、缓存过期时间
 *
 * @param <Key>
 *            - 缓存键对象
 * @param <Value>
 *            - 缓存值对象
 */
public class Cache<Key, Value>
{
    /**
     * 缓存容器
     */
    private ConcurrentHashMap<Key, CacheWrapper<Value>> container = new ConcurrentHashMap<Key, CacheWrapper<Value>>();

    /**
     * 最大缓存容量
     */
    private int maxCacheSize = 100;

    /**
     * 最小移除缓存数量
     */
    private int minRemoveSize = 5;

    /**
     * 过期时间
     */
    private long defaultExpire = 1800L; // 30 分钟

    /**
     * 常量, 100年
     */
    private final static long CenturyExpire = 3155695200L;

    /**
     * 缓存回调对象
     */
    private ICacheCallBack<Key, Value> cacheCallBack = null;
    
    /**
     * 
     */
    Timer timer = new Timer();
    
    public Cache()
    {
        this(100, CenturyExpire);
    }

    /**
     * Cache(int maxCacheSize)
     * 
     * @param maxCacheSize
     *            - 最大缓存容量
     */
    public Cache(int maxCacheSize)
    {
        this(maxCacheSize, CenturyExpire, null);
    }

    /**
     * Cache(long expire)
     * 
     * @param defaultExpire
     *            - 缺省过期时间, 单位：秒
     */
    public Cache(long defaultExpire)
    {
        this(100, defaultExpire, null);
    }

    /**
     * Cache(int maxCacheSize, long defaultExpire)
     * 
     * @param maxCacheSize
     *            - 最大缓存容量
     * @param defaultExpire
     *            - 缺省过期时间, 单位：秒
     */
    public Cache(int maxCacheSize, long defaultExpire)
    {
        this(maxCacheSize, defaultExpire, null);
    }

    /**
     * Cache(int maxCacheSize, long defaultExpire, ICacheCallBack<Key, Value>
     * cacheCallBack)
     * 
     * @param maxCacheSize
     *            - 最大缓存容量
     * @param defaultExpire
     *            - 缺省过期时间, 单位：秒
     * @param cacheCallBack
     *            - 缓存回调接口
     */
    public Cache(int maxCacheSize, long defaultExpire, ICacheCallBack<Key, Value> cacheCallBack)
    {
        this.maxCacheSize = maxCacheSize;
        this.defaultExpire = (defaultExpire > 0) ? defaultExpire : CenturyExpire;
        this.cacheCallBack = cacheCallBack;
        this.timer.schedule(new TimerTask(){

			@Override
			public void run() {
				release();	
			}
        	
        }, 0, 10);
    }

    /**
     * 取缓存对象
     * 
     * @param key
     * @return
     */
    public Value get(Key key)
    {
        Value cacheValue = null;

        if (key == null)
        {
            return null;
        }

        if (this.container.containsKey(key))
        {
            CacheWrapper<Value> cacheWrapper = this.container.get(key);

			cacheWrapper.count++;
			cacheValue = cacheWrapper.cacheValue;

        }

        return cacheValue;
    }

	//the hook should emit data
	public void remove(Key key){
		CacheWrapper<Value> value = this.container.remove(key);
		if(this.cacheCallBack != null){
			cacheCallBack.hook(key,value.cacheValue);
		}
	}
	
    /**
     * 加入缓存
     * 
     * @param key
     *            - 索引键
     * @param value
     *            - 缓存数据对象
     */
    public void put(Key key, Value value)
    {
        this.addCache(key, value, this.defaultExpire);
    }

	/**
     * 加入缓存
     * 
     * @param key
     *            - 索引键
     * @param value
     *            - 缓存数据对象
     * @param expire
     *            - 相对过期时间，单位：秒
     */
    public void put(Key key, Value value, long expire)
    {
        this.addCache(key, value, (expire > 0) ? expire : CenturyExpire);
    }

    /**
     * 数据对象加入缓存
     * 
     * @param key
     *            - 索引键
     * @param value
     *            - 缓存数据对象
     * @param expire
     *            - 相对过期时间，单位：秒
     */
    private void addCache(Key key, Value value, long expire)
    {
        // 参数检查
        if ((key != null) && (value != null))
        {
            long absoluteExpiration = this.currentTimestamp() + expire;

            // 不检查指定键的缓存对象是否存在，直接put
			//不存在key，hashmap会创建，存在则更新value
			//value是新建的cacheWrapper，因此更新时，过期时间也会更新
            
			CacheWrapper<Value> cacheWrapper = new CacheWrapper<Value>(value, absoluteExpiration);
			this.container.put(key, cacheWrapper);

        }
    }

    /**
     * 清空缓存
     */
    public void clear()
    {
        this.container.clear();
    }

    /**
     * 缓存对象数量
     * 
     * @return
     */
    public int size()
    {
        return this.container.size();
    }

    /**
     * 释放缓存空间
     * 
     */
    private void release()
    {
        ArrayList<Map.Entry<Key, CacheWrapper<Value>>> tmpList = new ArrayList<Map.Entry<Key, CacheWrapper<Value>>>(
                this.container.entrySet());

		for(Map.Entry<Key, CacheWrapper<Value>> entry:tmpList){
			if(entry.getValue().absoluteExpiration > this.currentTimestamp()){
				this.remove(entry.getKey());
			}
		}

    }

    /**
     * 获取当前时间戳
     * 
     * @return
     */
    private long currentTimestamp()
    {
        // 内部使用，不考虑时区问题
        return (System.currentTimeMillis() / 1000L);
    }

    /**
     * CacheWrapper<T> 缓存数据包装类 - 泛型，Cache内部类
     *
     */
    private class CacheWrapper<T>
    {
        private CacheWrapper(T cacheValue, long absoluteExpiration)
        {
            this.cacheValue = cacheValue;
            this.count = 0;
            this.absoluteExpiration = absoluteExpiration;
        }
		
		//版本控制，防止get到的数据被删除而重写
		private int version;

        /**
         * 访问记数器
         */
        private long count;

        /**
         * 绝对过期时间（时戳）
         */
        private long absoluteExpiration;

        /**
         * 缓存数据值对象
         */
        private T cacheValue;
    }

}
