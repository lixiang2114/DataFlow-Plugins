package com.df.plugin.sink.amqp.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.amqp.config.AmqpConfig;
import com.df.plugin.sink.amqp.dto.AmqpMapper;
import com.df.plugin.sink.amqp.util.AmqpUtil;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description AMQP服务模块
 */
@SuppressWarnings("unchecked")
public class AmqpService {
	/**
	 * AMQP客户端工具
	 */
	private AmqpUtil amqpUtil;
	
	/**
	 * AMQP客户端配置
	 */
	private AmqpConfig amqpConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(AmqpService.class);
	
	public AmqpService(){}
	
	public AmqpService(AmqpConfig amqpConfig){
		this.amqpConfig=amqpConfig;
		this.amqpUtil=amqpConfig.amqpUtil;
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean preSend() throws Exception {
		if(amqpConfig.preFailSinkSet.isEmpty())  return true;
		for(Object object:amqpConfig.preFailSinkSet) {
			AmqpMapper amqpMapper=(AmqpMapper)object;
			boolean isSuc=send(amqpMapper.exchange,amqpMapper.routingKey,amqpMapper.message);
			if(isSuc) return false;
			amqpConfig.preFailSinkSet.remove(object);
		}
		return true;
	}
	
	/**
	 * 发送消息
	 * @param msg 消息内容
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	public boolean sendMessage(String msg) throws Exception{
		String[] structMsg=parseMsg(msg,amqpConfig);
		
		String exchange=structMsg[0];
		String routingKey=structMsg[1];
		String message=structMsg[2];
		
		boolean loop=send(exchange, routingKey, message);
		if(loop) {
			log.error("send message to amqp server occur excepton...");
			amqpConfig.preFailSinkSet.add(new AmqpMapper(exchange,routingKey,message));
		}
		
		return !loop;
	}
	
	/**
	 * 发送消息
	 * @param exchange 交换器
	 * @param routingKey 路由键
	 * @param message 消息内容
	 * @return 是否发送成功(false:成功,true:失败)
	 * @throws Exception
	 */
	private boolean send(String exchange, String routingKey, String message) throws Exception{
		boolean loop=false;
		int times=0;
		do{
			try{
				Boolean isSuc=amqpUtil.commonSend(exchange, routingKey, message);
				loop=(null==isSuc||isSuc.booleanValue())?false:true;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(amqpConfig.failMaxWaitMills);
				log.error("send occur excepton: ",e);
			}
		}while(loop && times<amqpConfig.maxRetryTimes);
		return loop;
	}
	
	/**
	 * 解析通道消息行
	 * @param line 消息记录
	 * @return AMQP结构化记录
	 */
	private static final String[] parseMsg(String line,AmqpConfig config) {
		String[] retArr=new String[3];
		if(config.parse) {
			String[] fieldValArray=config.fieldSeparator.split(line);
			if(3>fieldValArray.length) {
				retArr[0]=config.defaultExchange;
				retArr[1]=config.defaultRoutingKey;
				retArr[2]=line;
				return retArr;
			}
			
			String exchangeStr="";
			String routingKeyStr="";
			ArrayList<String> fieldValList=new ArrayList<String>(Arrays.asList(fieldValArray));
			
			if(config.exchangeIndex<config.routingKeyIndex) {
				routingKeyStr=fieldValList.remove(config.routingKeyIndex).trim();
				exchangeStr=fieldValList.remove(config.exchangeIndex).trim();
			}else{
				exchangeStr=fieldValList.remove(config.exchangeIndex).trim();
				routingKeyStr=fieldValList.remove(config.routingKeyIndex).trim();
			}
			
			retArr[0]=exchangeStr.isEmpty()?config.defaultExchange:exchangeStr;
			retArr[1]=routingKeyStr.isEmpty()?config.defaultRoutingKey:routingKeyStr;
			retArr[2]=CommonUtil.joinToString(fieldValList).trim();
			return retArr;
		}else{
			if(CommonUtil.isNotDictJson(line)) {
				retArr[0]=config.defaultExchange;
				retArr[1]=config.defaultRoutingKey;
				retArr[2]=line;
				return retArr;
			}
			
			HashMap<String,Object> recordMap=CommonUtil.jsonStrToJava(line, HashMap.class);
			String exchangeStr=(String)recordMap.remove(config.exchangeField);
			String routingKeyStr=(String)recordMap.remove(config.routingKeyField);
			String record=CommonUtil.javaToJsonStr(recordMap);
			
			retArr[0]=exchangeStr.isEmpty()?config.defaultExchange:exchangeStr;
			retArr[1]=routingKeyStr.isEmpty()?config.defaultRoutingKey:routingKeyStr;
			retArr[2]=record.trim();
			return retArr;
		}
	}
}
