package com.df.plugin.sink.server;

import java.io.File;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.server.config.ContextConfig;
import com.df.plugin.sink.server.config.ServerProtocol;
import com.df.plugin.sink.server.handler.ServerHandler;
import com.df.plugin.sink.server.service.BufferService;
import com.df.plugin.sink.server.service.HttpService;
import com.df.plugin.sink.server.service.TcpService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;
import com.github.lixiang2114.flow.scheduler.SchedulerPool;
import com.github.lixiang2114.netty.Server;
import com.github.lixiang2114.netty.context.ServerConfig;
import com.github.lixiang2114.netty.server.HttpServer;
import com.github.lixiang2114.netty.server.TcpServer;

/**
 * @author Lixiang
 * @description Server发送器
 */
public class ServerSink extends SinkPluginAdapter {
	/**
	 * 嵌入式服务器
	 */
	private Server server;
	
	/**
	 * 上下文配置
	 */
	private ContextConfig config;
	
	/**
	 * 缓冲器服务配置
	 */
	private BufferService bufferService;
	
	/**
	 * 发送服务操作句柄
	 */
	private Future<?> sinkServerFuture;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ServerSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("ServerSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.config=new ContextConfig(flow).config();
		this.bufferService=new BufferService(config);
		
		if(ServerProtocol.TCP==config.protocol) {
			this.server=new TcpServer(new ServerConfig(config.port,config,TcpService.class));
		}else{
			this.server=new HttpServer(new ServerConfig(config.port,config,HttpService.class));
		}
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("ServerSink plugin handing...");
		if(flow.sinkStart) {
			log.info("ServerSink is already started...");
			return true;
		}
		
		sinkServerFuture=SchedulerPool.getTaskExecutor().submit(new ServerHandler(server));
		flow.sinkStart=true;
		
		try{
			while(flow.sinkStart) {
				String message=filterToSinkChannel.get(config.maxBatchWaitMills);
				if(null==message || (message=message.trim()).isEmpty()) continue;
				if(!bufferService.writeBuffer(message)) return false;
			}
		}catch(Exception e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		server.shutdownServer();
		bufferService.stopBuffer();
		sinkServerFuture.cancel(true);
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("ServerSink plugin config...");
		if(null==params || 0==params.length) return config.collectRealtimeParams();
		if(params.length<2) return config.getFieldValue((String)params[0]);
		return config.setFieldValue((String)params[0],params[1]);
	}
}
