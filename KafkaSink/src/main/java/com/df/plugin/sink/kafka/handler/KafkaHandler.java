package com.df.plugin.sink.kafka.handler;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.kafka.config.KafkaConfig;

/**
 * @author Lixiang
 * @description Kafka操作句柄
 */
@SuppressWarnings("unused")
public class KafkaHandler implements Callback{
	/**
	 * 是否发送成功
	 */
	public boolean isSuccess=true;
	
	/**
	 * Kafka客户端配置
	 */
	private KafkaConfig kafkaConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(KafkaHandler.class);
	
	public KafkaHandler(){}
	
	public KafkaHandler(KafkaConfig kafkaConfig){
		this.kafkaConfig=kafkaConfig;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception e) {
		if (null==e) return;
		isSuccess=false;
		Long offset=null;
		String topic=metadata.topic();
		int partitionIndex=metadata.partition();
		if(metadata.hasOffset()) offset=metadata.offset();
		String errInfo=new StringBuilder("")
				.append("topicName").append("=").append(topic)
				.append(",partitionIndex=").append(partitionIndex)
				.append(",offsetIndex=").append(offset).toString();
		log.error("send message to "+errInfo+" occur error...",e);
	}
}
