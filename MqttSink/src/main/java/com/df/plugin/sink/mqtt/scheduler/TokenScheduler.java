package com.df.plugin.sink.mqtt.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.mqtt.config.MqttConfig;

/**
 * @author Louis(Lixiang)
 * @description Token调度器
 */
public class TokenScheduler {
	/**
	 * Mqtt客户端配置
	 */
	private MqttConfig mqttConfig;
	
	/**
	 * Token过期事件调度池句柄
	 */
	private ScheduledFuture<?> future;
	
	/**
	 * Token过期事件调度池
	 */
	private ScheduledExecutorService tokenExpireService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(TokenScheduler.class);
	
	public TokenScheduler(){}
	
	public TokenScheduler(MqttConfig mqttConfig){
		this.mqttConfig=mqttConfig;
	}
	
	public void startTokenScheduler(){
		tokenExpireService = Executors.newSingleThreadScheduledExecutor();
		future = tokenExpireService.scheduleWithFixedDelay(new Runnable() {
			public void run() {
            	String token=null;
				try {
					token = mqttConfig.getTokenValue();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				
            	if(!mqttConfig.tokenFromPass){
            		mqttConfig.mqttConnectOptions.setUserName(token);
            		return;
            	}
            	
            	mqttConfig.mqttConnectOptions.setPassword(token.toCharArray());
            }
        },10, 10, TimeUnit.MINUTES);
	}
	
	/**
	 * 停止处理器
	 */
	public void stopTokenScheduler(){
		 if (future != null) future.cancel(true);
		 if (null == tokenExpireService) return;
		 tokenExpireService.shutdownNow();
		 log.info("token scheduler is stopping...");
	}
}
