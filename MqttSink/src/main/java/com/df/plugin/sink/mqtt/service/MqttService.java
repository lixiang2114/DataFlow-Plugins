package com.df.plugin.sink.mqtt.service;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.mqtt.config.MqttConfig;
import com.df.plugin.sink.mqtt.dto.TopicMapper;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description Mqtt服务
 */
@SuppressWarnings("unchecked")
public class MqttService {
	/**
	 * Mqtt客户端
	 */
	public MqttClient mqttClient;
	
	/**
	 * Mqtt配置
	 */
	private MqttConfig mqttConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MqttService.class);
	
	public MqttService(){}
	
	public MqttService(MqttConfig mqttConfig){
		this.mqttConfig=mqttConfig;
		this.mqttClient=mqttConfig.mqttClient;
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean preSend() throws InterruptedException {
		if(mqttConfig.preFailSinkSet.isEmpty())  return true;
		for(Object object:mqttConfig.preFailSinkSet) {
			TopicMapper topicMapper=(TopicMapper)object;
			topicMapper.setMqttConfig(mqttConfig);
			if(!topicMapper.send()) return false;
			mqttConfig.preFailSinkSet.remove(object);
		}
		return true;
	}
	
	/**
	 * 发送Mqtt消息
	 * @param msg 消息内容
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	public Boolean sendMsg(String msg) throws InterruptedException{
		String[] topicAndMsg=parseMsg(msg,mqttConfig);
		MqttMessage message = new MqttMessage(topicAndMsg[1].getBytes(StandardCharsets.UTF_8));
		message.setRetained(mqttConfig.getRetained());
		message.setQos(mqttConfig.getQos());
		
		boolean loop=false;
		int times=0;
		do{
			try{
				mqttClient.publish(topicAndMsg[0], message);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(mqttConfig.failMaxWaitMills);
				log.error("publish occur excepton: "+e.getMessage());
			}
		}while(loop && times<mqttConfig.maxRetryTimes);
		
		if(loop) mqttConfig.preFailSinkSet.add(new TopicMapper(topicAndMsg[0],message));
		return !loop;
	}
	
	/**
	 * 解析通道消息行
	 * @param line 消息记录
	 * @return 主题与消息
	 */
	private static final String[] parseMsg(String line,MqttConfig config) {
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
