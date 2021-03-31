package com.df.plugin.sink.mqtt.handler;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.mqtt.config.MqttConfig;

/**
 * @author Lixiang
 * @description Mqtt协议操作句柄
 */
@SuppressWarnings("unused")
public class MqttHandler implements MqttCallbackExtended{
	/**
	 * MQTT客户端配置
	 */
	private MqttConfig mqttConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MqttHandler.class);
	
	public MqttHandler(){}
	
	public MqttHandler(MqttConfig mqttConfig){
		this.mqttConfig=mqttConfig;
	}
	
	@Override
	public void connectionLost(Throwable cause) {
		log.error("MqttSink客户端断开连接...");
		cause.printStackTrace();
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {}

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {}
}
