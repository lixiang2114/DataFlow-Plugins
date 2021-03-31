package com.df.plugin.sink.kafka.service;

import java.util.HashMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.kafka.config.KafkaConfig;
import com.df.plugin.sink.kafka.handler.KafkaHandler;
import com.df.plugin.sink.kafka.util.TopicUtil;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description Kafka服务
 */
@SuppressWarnings("unchecked")
public class KafkaService {
	/**
	 * 主题工具
	 */
	private TopicUtil topicUtil;
	
	/**
	 * Kafka配置
	 */
	private KafkaConfig kafkaConfig;
	
	/**
	 * KafKafa客户端回调句柄
	 */
	private KafkaHandler kafkaHandler;
	
	/**
	 * KafKa生产者
	 */
	public KafkaProducer<String, String> producer;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(KafkaService.class);
	
	public KafkaService(){}
	
	public KafkaService(KafkaConfig kafkaConfig){
		this.kafkaConfig=kafkaConfig;
		this.producer=kafkaConfig.producer;
		this.kafkaHandler=new KafkaHandler();
		this.topicUtil=new TopicUtil(kafkaConfig.adminClient);
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean preSend() throws InterruptedException {
		if(kafkaConfig.preFailSinkSet.isEmpty())  return true;
		for(Object object:kafkaConfig.preFailSinkSet) {
			producer.send((ProducerRecord<String,String>)object, kafkaHandler);
			if(!kafkaHandler.isSuccess) return false;
			kafkaConfig.preFailSinkSet.remove(object);
		}
		return true;
	}
	
	/**
	 * 发送Kafka消息
	 * @param msg 消息内容
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	public Boolean sendMsg(String msg) throws Exception{
		String topic=null;
		String[] topicAndMsg=parseMsg(msg,kafkaConfig);
		
		if(kafkaConfig.autoCreateTopic) {
			topic=topicUtil.getTopic(topicAndMsg[0],kafkaConfig.partitionNumPerTopic);
		}else{
			topic=topicUtil.getTopic(topicAndMsg[0],kafkaConfig.partitionNumPerTopic,false);
		}
		
		if(null==topic) {
			log.error("Topic is NULL...");
			throw new RuntimeException("Topic is NULL...");
		}
		
		ProducerRecord<String, String> messageRecord = new ProducerRecord<String, String>(topic, topicAndMsg[1]);
		producer.send(messageRecord, kafkaHandler);
		if(kafkaHandler.isSuccess) return true;
		
		kafkaConfig.preFailSinkSet.add(messageRecord);
		return false;
	}
	
	/**
	 * 解析通道消息行
	 * @param line 消息记录
	 * @return 主题与消息
	 */
	private static final String[] parseMsg(String line,KafkaConfig config) {
		String[] retArr=new String[2];
		if(config.getParse()) {
			int sepIndex=line.indexOf(config.getFieldSeparator());
			if(-1==sepIndex) {
				retArr[0]=config.getDefaultTopic();
				retArr[1]=line.trim();
				return retArr;
			}
			
			int topicIndex=config.getTopicIndex();
			String firstPart=line.substring(0, sepIndex).trim();
			String secondPart=line.substring(sepIndex+1).trim();
			
			if(0==topicIndex){
				retArr[0]=firstPart;
				retArr[1]=secondPart;
			}else{
				retArr[0]=secondPart;
				retArr[1]=firstPart;
			}
			
			return retArr;
		}else{
			HashMap<String,Object> recordMap=CommonUtil.jsonStrToJava(line, HashMap.class);
			String topic=(String)recordMap.remove(config.getTopicField());
			String record=CommonUtil.javaToJsonStr(recordMap);
			if(null==topic || topic.trim().isEmpty()) {
				retArr[0]=config.getDefaultTopic();
				retArr[1]=record.trim();
				return retArr;
			}
			
			retArr[0]=topic.trim();
			retArr[1]=record.trim();
			return retArr;
		}
	}
}
