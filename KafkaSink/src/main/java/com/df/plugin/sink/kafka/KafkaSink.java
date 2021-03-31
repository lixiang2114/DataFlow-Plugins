package com.df.plugin.sink.kafka;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.kafka.config.KafkaConfig;
import com.df.plugin.sink.kafka.service.KafkaService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description Kafka发送器
 */
public class KafkaSink extends SinkPluginAdapter{
	/**
	 * Kafka客户端配置
	 */
	private KafkaConfig kafkaConfig;
	
	/**
	 * Kafka服务
	 */
	private KafkaService kafkaService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(KafkaSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("KafkaSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.kafkaConfig=new KafkaConfig(flow).config();
		this.kafkaService=new KafkaService(kafkaConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("KafkaSink plugin handing...");
		if(flow.sinkStart) {
			log.info("KafkaSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		if(!kafkaService.preSend()) return false;
		
		try{
			String message=null;
			while(flow.sinkStart) {
				if(null==(message=filterToSinkChannel.get())) continue;
				if((message=message.trim()).isEmpty()) continue;
				
				Boolean flag=kafkaService.sendMsg(message);
				if(null!=flag && !flag) return false;
			}
		}catch(InterruptedException e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		kafkaConfig.producer.close(15, TimeUnit.SECONDS);
		kafkaConfig.adminClient.close(15, TimeUnit.SECONDS);
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("KafkaSink plugin config...");
		if(null==params || 0==params.length) return kafkaConfig.collectRealtimeParams();
		if(params.length<2) return kafkaConfig.getFieldValue((String)params[0]);
		return kafkaConfig.setFieldValue((String)params[0],params[1]);
	}
}
