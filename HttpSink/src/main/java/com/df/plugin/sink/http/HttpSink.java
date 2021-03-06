package com.df.plugin.sink.http;

import java.io.File;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.http.config.HttpConfig;
import com.df.plugin.sink.http.hander.HttpSinkHandler;
import com.df.plugin.sink.http.service.BufferService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;
import com.github.lixiang2114.flow.scheduler.SchedulerPool;

/**
 * @author Lixiang
 * @description HTTP发送器
 */
public class HttpSink extends SinkPluginAdapter{
	/**
	 * HTTP连接配置
	 */
	private HttpConfig config;
	
	/**
	 * 发送服务操作句柄
	 */
	private Future<?> httpSinkFuture;
	
	/**
	 * 缓冲器模块配置
	 */
	private BufferService bufferService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HttpSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("HttpSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.config=new HttpConfig(flow).config();
		this.bufferService=new BufferService(config);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("HttpSink plugin handing...");
		if(flow.sinkStart) {
			log.info("HttpSink is already started...");
			return true;
		}
		
		httpSinkFuture=SchedulerPool.getTaskExecutor().submit(new HttpSinkHandler(config));
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
		bufferService.stopBuffer();
		httpSinkFuture.cancel(true);
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("HttpSink plugin config...");
		if(null==params || 0==params.length) return config.collectRealtimeParams();
		if(params.length<2) return config.getFieldValue((String)params[0]);
		return config.setFieldValue((String)params[0],params[1]);
	}
}
