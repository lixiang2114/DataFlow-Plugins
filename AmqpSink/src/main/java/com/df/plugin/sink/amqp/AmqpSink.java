package com.df.plugin.sink.amqp;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.amqp.config.AmqpConfig;
import com.df.plugin.sink.amqp.dto.Route;
import com.df.plugin.sink.amqp.service.AmqpService;
import com.df.plugin.sink.amqp.util.AmqpUtil;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description Amqp发送器
 */
public class AmqpSink extends SinkPluginAdapter{
	/**
	 * Amqp客户端配置
	 */
	private AmqpConfig amqpConfig;
	
	/**
	 * Amqp服务模块
	 */
	private AmqpService amqpService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(AmqpSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("AmqpSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.amqpConfig=new AmqpConfig(flow).config();
		this.amqpService=new AmqpService(amqpConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("AmqpSink plugin handing...");
		if(flow.sinkStart) {
			log.info("AmqpSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		if(!amqpService.preSend()) return false;
		
		try{
			String message=null;
			while(flow.sinkStart) {
				if(null==(message=filterToSinkChannel.get())) continue;
				if((message=message.trim()).isEmpty()) continue;
				if(!amqpService.sendMessage(message)) return false;
			}
		}catch(InterruptedException e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		//销毁RMQ拓扑架构图
		AmqpUtil amqpUtil=amqpConfig.amqpUtil;
		for(Route route:amqpConfig.routeList) {
			amqpUtil.delBinding(route.binding);
			amqpUtil.delExchange(route.srcName);
			if("queue".equals(route.dstType)) {
				amqpUtil.delQueue(route.dstName);
			}else{
				amqpUtil.delExchange(route.dstName);
			}
			log.info("remove route and destory component: {} finished...",route);
		}
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("AmqpSink plugin config...");
		if(null==params || 0==params.length) return amqpConfig.collectRealtimeParams();
		if(params.length<2) return amqpConfig.getFieldValue((String)params[0]);
		return amqpConfig.setFieldValue((String)params[0],params[1]);
	}
}
