package com.df.plugin.transfer.amqp;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.amqp.config.AmqpConfig;
import com.df.plugin.transfer.amqp.dto.Route;
import com.df.plugin.transfer.amqp.service.AmqpService;
import com.df.plugin.transfer.amqp.util.AmqpUtil;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.TransferPluginAdapter;

/**
 * @author Lixiang
 * @description AMQP转存器
 */
public class AmqpTransfer extends TransferPluginAdapter {
	/**
	 * AMQP客户端配置
	 */
	private AmqpConfig amqpConfig;
	
	/**
	 * AMQP服务模块
	 */
	private AmqpService amqpService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(AmqpTransfer.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("AmqpTransfer plugin starting...");
		File confFile=new File(pluginPath,"transfer.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.amqpConfig=new AmqpConfig(flow).config();
		this.amqpService=new AmqpService(amqpConfig);
		
		return true;
	}
	
	@Override
	public Object transfer(Channel<String> transferToSourceChannel) throws Exception {
		log.info("AmqpTransfer plugin starting...");
		if(flow.transferStart) {
			log.info("AmqpTransfer is already started...");
			return true;
		}
		
		flow.transferStart=true;
		amqpService.startScanner();
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.transferStart=false;
		if(!amqpConfig.delRoutingOnExit) return true;
		
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
	public Object config(Object... params) throws Exception {
		log.info("AmqpTransfer plugin config...");
		if(null==params || 0==params.length) return amqpConfig.collectRealtimeParams();
		if(params.length<2) return amqpConfig.getFieldValue((String)params[0]);
		return amqpConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.warn("AmqpTransfer plugin not support reflesh checkpoint...");
		return true;
	}
}
