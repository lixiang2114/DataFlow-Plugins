package com.df.plugin.sink.elastic;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.elastic.config.ElasticConfig;
import com.df.plugin.sink.elastic.service.ElasticService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description Elastic发送器
 */
public class ElasticSink extends SinkPluginAdapter{
	/**
	 * Elastic配置
	 */
	private ElasticConfig elasticConfig;
	
	/**
	 * Elastic服务
	 */
	private ElasticService elasticService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ElasticSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("ElasticSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.elasticConfig=new ElasticConfig(flow).config();
		this.elasticService=new ElasticService(elasticConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("ElasticSink plugin handing...");
		if(flow.sinkStart) {
			log.info("ElasticSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		if(!elasticService.preSend()) return false;
		
		try{
			String message=null;
			if(elasticConfig.parse){
				if(null==elasticConfig.batchSize) {
					log.info("call parseAndSingleSend...");
					while(flow.sinkStart) {
						if(null==(message=filterToSinkChannel.get())) continue;
						if((message=message.trim()).isEmpty()) continue;
						Boolean flag=elasticService.parseAndSingleSend(message);
						if(null!=flag && !flag) return false;
					}
				}else{
					log.info("call parseAndBatchSend...");
					while(flow.sinkStart) {
						Boolean flag=elasticService.parseAndBatchSend(filterToSinkChannel.get(elasticConfig.batchMaxWaitMills));
						if(null!=flag && !flag) return false;
					}
				}
			}else{
				if(null==elasticConfig.batchSize) {
					log.info("call noParseAndSingleSend...");
					while(flow.sinkStart) {
						if(null==(message=filterToSinkChannel.get())) continue;
						if((message=message.trim()).isEmpty()) continue;
						Boolean flag=elasticService.noParseAndSingleSend(message);
						if(null!=flag && !flag) return false;
					}
				}else{
					log.info("call noParseAndBatchSend...");
					while(flow.sinkStart) {
						Boolean flag=elasticService.noParseAndBatchSend(filterToSinkChannel.get(elasticConfig.batchMaxWaitMills));
						if(null!=flag && !flag) return false;
					}
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
		return elasticService.stop();
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("ElasticSink plugin config...");
		if(null==params || 0==params.length) return elasticConfig.collectRealtimeParams();
		if(params.length<2) return elasticConfig.getFieldValue((String)params[0]);
		return elasticConfig.setFieldValue((String)params[0],params[1]);
	}
}
