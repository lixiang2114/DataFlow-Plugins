package com.df.plugin.transfer.kafka;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.kafka.config.KafkaConfig;
import com.df.plugin.transfer.kafka.service.KafkaService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.TransferPluginAdapter;

/**
 * @author Lixiang
 * @description Kafka转存器
 */
public class KafkaTransfer extends TransferPluginAdapter {
	/**
	 * Kafka客户端配置
	 */
	private KafkaConfig kafkaConfig;
	
	/**
	 * Kafka服务模块
	 */
	private KafkaService kafkaService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(KafkaTransfer.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("KafkaTransfer plugin starting...");
		File confFile=new File(pluginPath,"transfer.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.kafkaConfig=new KafkaConfig(flow).config();
		this.kafkaService=new KafkaService(kafkaConfig);
		
		return true;
	}
	
	@Override
	public Object transfer(Channel<String> transferToSourceChannel) throws Exception {
		log.info("KafkaTransfer plugin starting...");
		if(flow.transferStart) {
			log.info("KafkaTransfer is already started...");
			return true;
		}
		
		flow.transferStart=true;
		kafkaService.startScanner();
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		kafkaConfig.consumer.unsubscribe();
		kafkaConfig.consumer.close(Duration.ofSeconds(15));
		kafkaConfig.adminClient.close();
		flow.transferStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("KafkaTransfer plugin config...");
		if(null==params || 0==params.length) return kafkaConfig.collectRealtimeParams();
		if(params.length<2) return kafkaConfig.getFieldValue((String)params[0]);
		return kafkaConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.warn("KafkaTransfer plugin not support reflesh checkpoint...");
		return true;
	}
}
