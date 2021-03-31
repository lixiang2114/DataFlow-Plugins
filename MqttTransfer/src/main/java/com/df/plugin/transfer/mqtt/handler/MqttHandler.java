package com.df.plugin.transfer.mqtt.handler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.mqtt.config.MqttConfig;

/**
 * @author Lixiang
 * @description Mqtt协议操作句柄
 */
public class MqttHandler implements MqttCallbackExtended{
	/**
	 * MQTT客户端配置
	 */
	private MqttConfig mqttConfig;
	
	/**
	 * 转存缓冲输出器
	 */
	private BufferedWriter bufferedWriter;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MqttHandler.class);
	
	public MqttHandler(){}
	
	public MqttHandler(MqttConfig mqttConfig){
		this.mqttConfig=mqttConfig;
		try {
			bufferedWriter=Files.newBufferedWriter(mqttConfig.transferSaveFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		} catch (IOException e) {
			log.error("transferSaveFile is not exists: ",e);
		}
	}
	
	@Override
	public void connectionLost(Throwable cause) {
		log.error("MqttTransfer客户端断开连接...");
		cause.printStackTrace();
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		//写入转存日志文件
		bufferedWriter.write(new String(message.getPayload(),"UTF-8"));
		bufferedWriter.newLine();
		bufferedWriter.flush();
		
		//当前转存日志文件未达到最大值则继续写转存日志文件
		if(mqttConfig.transferSaveFile.length()<mqttConfig.transferSaveMaxSize) return;
		
		//当前转存日志文件达到最大值则增加转存日志文件
		String curTransSaveFilePath=mqttConfig.transferSaveFile.getAbsolutePath();
		int lastIndex=curTransSaveFilePath.lastIndexOf(".");
		if(-1==lastIndex) {
			lastIndex=curTransSaveFilePath.length();
			curTransSaveFilePath=curTransSaveFilePath+".0";
		}
		mqttConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
		log.info("MqttTransfer switch transfer save log file to: "+mqttConfig.transferSaveFile.getAbsolutePath());
		
		try{
			bufferedWriter.close();
		}catch(IOException e){
			log.error("close transferSaveFile stream occur error: ",e);
		}
		
		bufferedWriter=Files.newBufferedWriter(mqttConfig.transferSaveFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {}

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {}
}
