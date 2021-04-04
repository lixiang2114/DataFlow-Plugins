package com.df.plugin.sink.redis;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.redis.config.RedisConfig;
import com.df.plugin.sink.redis.config.RedisType;
import com.df.plugin.sink.redis.service.RedisService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description Redis发送器
 */
public class RedisSink extends SinkPluginAdapter{
	/**
	 * Redis客户端配置
	 */
	private RedisConfig redisConfig;
	
	/**
	 * Redis服务模块
	 */
	private RedisService redisService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(RedisSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("RedisSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.redisConfig=new RedisConfig(flow).config();
		this.redisService=new RedisService(redisConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("RedisSink plugin handing...");
		if(flow.sinkStart) {
			log.info("RedisSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		if(!redisService.preSend()) return false;
		
		try{
			String message=null;
			if(RedisType.dict==redisConfig.targetType){ //缓存数据
				while(flow.sinkStart) {
					if(null==(message=filterToSinkChannel.get())) continue;
					if((message=message.trim()).isEmpty()) continue;
					Boolean flag=redisService.sendDictMsg(message);
					if(null!=flag && !flag) return false;
				}
			}else{ //推送消息
				while(flow.sinkStart) {
					if(null==(message=filterToSinkChannel.get())) continue;
					if((message=message.trim()).isEmpty()) continue;
					Boolean flag=redisService.sendPipeMsg(message);
					if(null!=flag && !flag) return false;
				}
			}
		}catch(InterruptedException e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("RedisSink plugin config...");
		if(null==params || 0==params.length) return redisConfig.collectRealtimeParams();
		if(params.length<2) return redisConfig.getFieldValue((String)params[0]);
		return redisConfig.setFieldValue((String)params[0],params[1]);
	}
}
