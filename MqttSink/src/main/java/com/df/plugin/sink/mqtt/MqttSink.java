package com.df.plugin.sink.mqtt;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.mqtt.config.MqttConfig;
import com.df.plugin.sink.mqtt.scheduler.TokenScheduler;
import com.df.plugin.sink.mqtt.service.MqttService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description MQTT发送器
 */
public class MqttSink extends SinkPluginAdapter{
	/**
	 * MQTT客户端配置
	 */
	private MqttConfig mqttConfig;
	
	/**
	 * MQTT服务
	 */
	private MqttService mqttService;
	
	/**
	 * Token调度器
	 */
	private TokenScheduler tokenScheduler;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MqttSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("MqttSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.mqttConfig=new MqttConfig(flow).config();
		
		mqttConfig.connectMqttServer();
		this.mqttService=new MqttService(mqttConfig);
		
		if(null==mqttConfig.startTokenScheduler || !mqttConfig.startTokenScheduler){
			log.info("token expire is -1,no need to start the scheduler!");
		}else{
			tokenScheduler=new TokenScheduler(mqttConfig);
			tokenScheduler.startTokenScheduler();
			log.info("expire token scheduler is already started...");
		}
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("MqttSink plugin handing...");
		if(flow.sinkStart) {
			log.info("MqttSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		if(!mqttService.preSend()) return false;
		
		try{
			String message=null;
			while(flow.sinkStart) {
				if(null==(message=filterToSinkChannel.get())) continue;
				if((message=message.trim()).isEmpty()) continue;
				Boolean flag=mqttService.sendMsg(message);
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
		tokenScheduler.stopTokenScheduler();
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("MqttSink plugin config...");
		if(null==params || 0==params.length) return mqttConfig.collectRealtimeParams();
		if(params.length<2) return mqttConfig.getFieldValue((String)params[0]);
		return mqttConfig.setFieldValue((String)params[0],params[1]);
	}
}
