package com.df.plugin.transfer.mqtt;

import java.io.File;
import java.io.IOException;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.mqtt.config.MqttConfig;
import com.df.plugin.transfer.mqtt.scheduler.TokenScheduler;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.TransferPluginAdapter;

/**
 * @author Lixiang
 * @description MQTT转存器
 */
public class MqttTransfer extends TransferPluginAdapter {
	/**
	 * MQTT客户端
	 */
	private MqttClient mqttClient;
	
	/**
	 * MQTT客户端配置
	 */
	private MqttConfig mqttConfig;
	
	/**
	 * Token调度器
	 */
	private TokenScheduler tokenScheduler;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MqttTransfer.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("MqttTransfer plugin starting...");
		File confFile=new File(pluginPath,"transfer.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.mqttConfig=new MqttConfig(flow).config();
		return true;
	}
	
	@Override
	public Object transfer(Channel<String> transferToSourceChannel) throws Exception {
		log.info("MqttTransfer plugin starting...");
		if(flow.transferStart) {
			log.info("MqttTransfer is already started...");
			return true;
		}
		
		flow.transferStart=true;
		this.mqttClient=mqttConfig.connectMqttServer();
		
		if(null==mqttConfig.startTokenScheduler || !mqttConfig.startTokenScheduler){
			log.info("token expire is -1,no need to start the scheduler!");
		}else{
			tokenScheduler=new TokenScheduler(mqttConfig);
			tokenScheduler.startTokenScheduler();
			log.info("expire token scheduler is already started...");
		}
		
		mqttClient.subscribe(mqttConfig.getTopic(), mqttConfig.getQos());
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		if(null!=tokenScheduler)tokenScheduler.stopTokenScheduler();
		mqttClient.unsubscribe(mqttConfig.getTopic());
		mqttClient.disconnectForcibly();
		flow.transferStart=false;
		mqttClient.close(true);
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("MqttTransfer plugin config...");
		if(null==params || 0==params.length) return mqttConfig.collectRealtimeParams();
		if(params.length<2) return mqttConfig.getFieldValue((String)params[0]);
		return mqttConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.warn("MqttTransfer plugin not support reflesh checkpoint...");
		return true;
	}
}
