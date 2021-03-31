package com.df.plugin.sink.mqtt.dto;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.mqtt.config.MqttConfig;

/**
 * @author Lixiang
 * @description 主题映射器
 */
public class TopicMapper {
	/**
	 * Mqtt主题
	 */
	private String topic;
	
	/**
	 * Mqtt消息
	 */
	private MqttMessage message;
	
	/**
	 * Mqtt配置
	 */
	private MqttConfig mqttConfig;
	
	/**
	 * Mqtt客户端
	 */
	private MqttClient mqttClient;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(TopicMapper.class);
	
	public TopicMapper(String topic,MqttMessage message) {
		this.topic=topic;
		this.message=message;
	}
	
	public void setMqttConfig(MqttConfig mqttConfig) {
		this.mqttConfig=mqttConfig;
		this.mqttClient=mqttConfig.mqttClient;
	}
	
	public boolean send() throws InterruptedException {
		boolean loop=false;
		int times=0;
		do{
			try{
				mqttClient.publish(topic, message);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(mqttConfig.failMaxWaitMills);
				log.error("publish occur excepton: "+e.getMessage());
			}
		}while(loop && times<mqttConfig.maxRetryTimes);
		
		if(!loop) mqttConfig.preFailSinkSet.clear();
		return !loop;
	}
}
