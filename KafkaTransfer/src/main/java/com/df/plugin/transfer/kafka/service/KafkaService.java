package com.df.plugin.transfer.kafka.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.kafka.config.KafkaConfig;
import com.df.plugin.transfer.kafka.util.TopicUtil;

/**
 * @author Lixiang
 * @description Kafka服务模块
 */
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
	 * 转存缓冲输出器
	 */
	private BufferedWriter bufferedWriter;
	
	/**
	 * KafKa生产者
	 */
	public KafkaConsumer<String, String> consumer;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(KafkaService.class);
	
	public KafkaService() {}
	
	public KafkaService(KafkaConfig kafkaConfig) {
		this.kafkaConfig=kafkaConfig;
		this.consumer=kafkaConfig.consumer;
		this.topicUtil=new TopicUtil(kafkaConfig.adminClient);
		try {
			bufferedWriter=Files.newBufferedWriter(kafkaConfig.transferSaveFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		} catch (IOException e) {
			log.error("transferSaveFile is not exists: ",e);
		}
	}
	
	/**
	 * 启动扫描器
	 * @throws Exception
	 */
	public void startScanner() throws Exception {
		String topic=topicUtil.getTopic(kafkaConfig.topic, kafkaConfig.partitionNumPerTopic,kafkaConfig.autoCreateTopic);
		if(null==topic) {
			log.error("Topic is NULL...");
			throw new RuntimeException("Topic is NULL...");
		}
		
		consumer.subscribe(Arrays.asList(topic));
		Duration batchTimeout=kafkaConfig.batchPollTimeoutMills;
		
		while(kafkaConfig.flow.transferStart) {
			 	//拉取一个批次的数据记录量
	            ConsumerRecords<String, String> batchRecords=consumer.poll(batchTimeout);
	            for (ConsumerRecord<String, String> record:batchRecords) {
	            	if(null==record) continue;
	            	
	            	String line=record.value();
	            	if(null==line || (line=line.trim()).isEmpty()) continue;
	            	
	            	//写入转存日志文件
	            	bufferedWriter.write(line);
	            	bufferedWriter.newLine();
	            	bufferedWriter.flush();
	            	
	            	//当前转存日志文件未达到最大值则继续写转存日志文件
	            	if(kafkaConfig.transferSaveFile.length()<kafkaConfig.transferSaveMaxSize) continue;
	            	
	            	//当前转存日志文件达到最大值则增加转存日志文件
	            	String curTransSaveFilePath=kafkaConfig.transferSaveFile.getAbsolutePath();
	            	int lastIndex=curTransSaveFilePath.lastIndexOf(".");
	            	if(-1==lastIndex) {
						lastIndex=curTransSaveFilePath.length();
						curTransSaveFilePath=curTransSaveFilePath+".0";
					}
	            	kafkaConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
	            	log.info("KafkaTransfer switch transfer save log file to: "+kafkaConfig.transferSaveFile.getAbsolutePath());
	            	
	            	try{
	            		bufferedWriter.close();
	            	}catch(IOException e){
	            		log.error("close transferSaveFile stream occur error: ",e);
	            	}
	            	
	            	bufferedWriter=Files.newBufferedWriter(kafkaConfig.transferSaveFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,StandardOpenOption.APPEND);
	            }
	    }
	}
}
