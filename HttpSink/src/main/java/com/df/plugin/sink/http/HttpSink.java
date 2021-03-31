package com.df.plugin.sink.http;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.http.config.HttpConfig;
import com.df.plugin.sink.http.service.HttpService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description Http发送器
 */
public class HttpSink extends SinkPluginAdapter{
	/**
	 * Http连接配置
	 */
	private HttpConfig httpConfig;
	
	/**
	 * Http服务操作
	 */
	private HttpService httpService;
	
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
		
		this.httpConfig=new HttpConfig(flow).config();
		this.httpService=new HttpService(httpConfig);
		
		return httpService.login();
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("HttpSink plugin handing...");
		if(flow.sinkStart) {
			log.info("HttpSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		if(!httpService.prePost()) return false;
		
		try{
			String message=null;
			while(flow.sinkStart) {
				if(null==(message=filterToSinkChannel.get())) continue;
				if((message=message.trim()).isEmpty()) continue;
				if(!httpService.post(message)) return false;
			}
		}catch(Exception e){
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
		log.info("HttpSink plugin config...");
		if(null==params || 0==params.length) return httpConfig.collectRealtimeParams();
		if(params.length<2) return httpConfig.getFieldValue((String)params[0]);
		return httpConfig.setFieldValue((String)params[0],params[1]);
	}
}
