package com.alibaba.middleware.race.util;

/**
 * 缓存回调接口定义
 *
 */
public interface ICacheCallBack<Key, Value>
{
    Value getObjectCallBack(Key key);

	void hook(Key key, Value cacheValue);
	
}
