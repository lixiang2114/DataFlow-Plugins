package com.df.plugin.transfer.redis;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.redis.config.RedisConfig;
import com.df.plugin.transfer.redis.service.RedisService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.TransferPluginAdapter;

/**
 * @author Lixiang
 * @description Redis转存器
 */
public class RedisTransfer extends TransferPluginAdapter {
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
	private static final Logger log=LoggerFactory.getLogger(RedisTransfer.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("RedisTransfer plugin starting...");
		File confFile=new File(pluginPath,"transfer.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.redisConfig=new RedisConfig(flow).config();
		this.redisService=new RedisService(redisConfig);
		
		return true;
	}
	
	@Override
	public Object transfer(Channel<String> transferToSourceChannel) throws Exception {
		log.info("RedisTransfer plugin starting...");
		if(flow.transferStart) {
			log.info("RedisTransfer is already started...");
			return true;
		}
		
		flow.transferStart=true;
		redisService.startScanner();
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.transferStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("RedisTransfer plugin config...");
		if(null==params || 0==params.length) return redisConfig.collectRealtimeParams();
		if(params.length<2) return redisConfig.getFieldValue((String)params[0]);
		return redisConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.warn("RedisTransfer plugin not support reflesh checkpoint...");
		return true;
	}
}
