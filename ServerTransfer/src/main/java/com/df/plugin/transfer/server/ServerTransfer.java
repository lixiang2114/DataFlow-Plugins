package com.df.plugin.transfer.server;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.server.config.ServerConfig;
import com.df.plugin.transfer.server.service.ServerService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.TransferPluginAdapter;

/**
 * @author Lixiang
 * @description 基于服务的Transfer插件
 */
public class ServerTransfer extends TransferPluginAdapter {
	/**
	 * ServerSource配置
	 */
	private ServerConfig serverConfig;
	
	/**
	 * ServerService服务组件
	 */
	private ServerService serverService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ServerTransfer.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("ServerTransfer plugin starting...");
		File confFile=new File(pluginPath,"transfer.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.serverConfig=new ServerConfig(flow).config();
		serverService=new ServerService(serverConfig);
		
		return true;
	}
	
	@Override
	public Object transfer(Channel<String> transferToSourceChannel) throws Exception {
		log.info("ServerTransfer plugin starting...");
		if(flow.transferStart) {
			log.info("ServerTransfer is already started...");
			return true;
		}
		
		flow.transferStart=true;
		return serverService.startTransferSave(transferToSourceChannel);
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		return serverService.stopTransferSave(params);
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("ServerTransfer plugin config...");
		if(null==params || 0==params.length) return serverConfig.collectRealtimeParams();
		if(params.length<2) return serverConfig.getFieldValue((String)params[0]);
		return serverConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新文件检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.info("ServerTransfer plugin reflesh checkpoint...");
		serverConfig.refreshCheckPoint();
		return true;
	}
}
