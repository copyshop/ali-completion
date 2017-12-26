package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

	private DefaultTairManager tairManager;
	private Integer TairNamespace = 0;
	
    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
    	
    	//
    	TairNamespace = namespace;
    	
    	// 创建config server列表
    	List<String> confServers = new ArrayList<String>();
    	confServers.add(masterConfigServer);

    	// 创建客户端实例
    	tairManager = new DefaultTairManager();
    	tairManager.setConfigServerList(confServers);
    	// 设置组名
    	tairManager.setGroupName(groupName);
    	// 初始化客户端
    	tairManager.init();
    }

    public int incr(Serializable key, int value){
    	Result<Integer> result =tairManager.incr(TairNamespace, key, value, 0, 0);
    	if(result != null && result.isSuccess()){
    		return result.getValue();
    	}else{
    		return -1;
    	}
    }
    
    public boolean write(Serializable key, Serializable value) {
    	ResultCode rc = tairManager.put(TairNamespace, key, value);
    	return rc.isSuccess();
    }

    public Result<DataEntry> get(Serializable key) {
        return tairManager.get(TairNamespace, key);
    }

    public boolean remove(Serializable key) {
        return false;
    }

    public void close(){
    	tairManager.close();
    }

    
    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
//        if(tairOperator.write(RaceConfig.prex_tmall + minuteTime, money)){
//        	System.out.println("put success");
//        };
//        String key = "platformTmall_1465941361";
//        System.out.println(tairOperator.incr(key, 100));
        System.out.println(tairOperator.get("platformTmall_1467981660"));
    }
}
