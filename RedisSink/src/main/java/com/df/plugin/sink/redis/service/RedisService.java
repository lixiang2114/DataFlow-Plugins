package com.df.plugin.sink.redis.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.redis.config.RedisConfig;
import com.df.plugin.sink.redis.config.RedisType;
import com.df.plugin.sink.redis.dto.RedisMapper;
import com.df.plugin.sink.redis.util.RedisUtil;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description Redis服务模块
 */
@SuppressWarnings("unchecked")
public class RedisService {
	/**
	 * Redis客户端工具
	 */
	private RedisUtil redisUtil;
	
	/**
	 * Redis客户端配置
	 */
	private RedisConfig redisConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(RedisService.class);
	
	public RedisService(){}
	
	public RedisService(RedisConfig redisConfig){
		this.redisConfig=redisConfig;
		this.redisUtil=redisConfig.redisUtil;
	}
	
	/**
	 * 预发送数据(将上次反馈为失败的数据重新发送)
	 * @return 本方法执行后,预发表是否为空(true:空,false:非空)
	 * @throws InterruptedException
	 */
	public boolean preSend() throws Exception {
		if(redisConfig.preFailSinkSet.isEmpty())  return true;
		for(Object object:redisConfig.preFailSinkSet) {
			RedisMapper redisMapper=(RedisMapper)object;
			boolean isSuc=false;
			if(RedisType.dict==redisMapper.targetType) {
				isSuc=sendMapMsg(redisMapper.target,(HashMap<String,Object>)redisMapper.message);
			}else{
				isSuc=sendRecord(redisMapper.target, (Object[])redisMapper.message);
			}
			
			if(!isSuc) return false;
			redisConfig.preFailSinkSet.remove(object);
		}
		return true;
	}
	
	/**
	 * 发送目标字典消息
	 * @param msg 消息内容
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	public Boolean sendDictMsg(String msg) throws Exception{
		String[] targetAndMsg=parseMsg(msg,redisConfig);
		
		String target=targetAndMsg[0];
		String message=targetAndMsg[1];
		
		if(target.isEmpty() || message.isEmpty()) {
			log.error("target and  message can not be empty...");
			return true;
		}
		
		if(CommonUtil.isNotDictJson(message)) {
			log.error("message: {} is not an dictory...",message);
			return true;
		}
		
		HashMap<String,Object> messageMap=CommonUtil.jsonStrToJava(message, HashMap.class);
		if(null==messageMap || messageMap.isEmpty()) {
			log.error("dict message: {} transfer to messageMap is null or empty...",message);
			return true;
		}
		
		for(Entry<String, Object> entry:messageMap.entrySet()) {
			Object value=entry.getValue();
			if(null==value) continue;
			Object json=CommonUtil.toJSON(value);
			Class<?> jsonType=json.getClass();
			if(CommonUtil.isSimpleType(jsonType)) continue;
			messageMap.put(entry.getKey(), CommonUtil.javaToJsonStr(json).trim());
		}
		
		return sendMapMsg(target,messageMap);
	}
	
	/**
	 * 发送字典消息
	 * @param target 目标字典
	 * @param messageMap 字典对象
	 * @return 是否发送成功
	 * @throws Exception
	 */
	private boolean sendMapMsg(String target,HashMap<String,Object> messageMap) throws Exception{
		ArrayList<String> sucKeys=new ArrayList<String>();
		if("all".equals(target)) {
			for(Entry<String, Object> entry:messageMap.entrySet()) {
				String key=entry.getKey();
				if(!singleSendMap(key,entry.getValue())) break;
				sucKeys.add(key);
			}
		}else{
			if(batchSendMap(target, messageMap)) sucKeys.addAll(messageMap.keySet());
		}
		
		if(sucKeys.size()==messageMap.size()) return true;
		
		for(String sucKey:sucKeys) messageMap.remove(sucKey);
		redisConfig.preFailSinkSet.add(new RedisMapper(target,messageMap,redisConfig.targetType));
		
		return false;
	}
	
	/**
	 * 发送单个键值对
	 * @param target 目标对象
	 * @param messageMap 消息字典
	 * @return 是否发送成功
	 * @throws Exception
	 */
	private boolean singleSendMap(String target,Object message) throws Exception{
		boolean loop=false;
		int times=0;
		do{
			try{
				redisUtil.set(target, message);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(redisConfig.failMaxWaitMills);
				log.error("send occur excepton: ",e);
			}
		}while(loop && times<redisConfig.maxRetryTimes);
		return !loop;
	}
	
	/**
	 * 发送批量键值对
	 * @param target 目标对象
	 * @param messageMap 消息字典
	 * @return 是否发送成功
	 * @throws Exception
	 */
	private boolean batchSendMap(String target,HashMap<String,Object> messageMap) throws Exception{
		boolean loop=false;
		int times=0;
		do{
			try{
				redisUtil.hashSet(target, messageMap);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(redisConfig.failMaxWaitMills);
				log.error("send occur excepton: ",e);
			}
		}while(loop && times<redisConfig.maxRetryTimes);
		return !loop;
	}
	
	/**
	 * 发送目标管道消息
	 * @param msg 消息内容
	 * @return 是否发送成功
	 * @throws InterruptedException
	 */
	public Boolean sendPipeMsg(String msg) throws Exception{
		String[] targetAndMsg=parseMsg(msg,redisConfig);
		
		String target=targetAndMsg[0];
		String message=targetAndMsg[1];
		
		if(target.isEmpty() || message.isEmpty()) {
			log.error("target and  message can not be empty...");
			return true;
		}
		
		String[] messages=redisConfig.fieldSeparatorRegex.split(message);
		for(int i=0;i<messages.length;messages[i]=messages[i++].trim());
		
		if(sendRecord(target, (Object[])messages)) return true;
		redisConfig.preFailSinkSet.add(new RedisMapper(target,messages,redisConfig.targetType));
		
		return false;
	}
	
	/**
	 * 批量发送记录到目标管道
	 * @param target 目标管道
	 * @param messages 消息内容
	 * @return 是否发送成功
	 * @throws Exception
	 */
	private boolean sendRecord(String target,Object... messages) throws Exception{
		boolean loop=false;
		int times=0;
		do{
			try{
				redisUtil.leftPush(target, messages);
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(redisConfig.failMaxWaitMills);
				log.error("send occur excepton: ",e);
			}
		}while(loop && times<redisConfig.maxRetryTimes);
		return !loop;
	}
	
	/**
	 * 解析通道消息行
	 * @param line 消息记录
	 * @return Redis对象与缓存数据
	 */
	private static final String[] parseMsg(String line,RedisConfig config) {
		String[] retArr=new String[2];
		if(config.parse) {
			int sepIndex=line.indexOf(config.fieldSeparator);
			if(-1==sepIndex) {
				retArr[0]=config.defaultTarget;
				retArr[1]=line.trim();
				return retArr;
			}
			
			int targetIndex=config.targetIndex;
			String firstPart=line.substring(0, sepIndex).trim();
			String secondPart=line.substring(sepIndex+1).trim();
			
			if(0==targetIndex){
				retArr[0]=firstPart;
				retArr[1]=secondPart;
			}else{
				retArr[0]=secondPart;
				retArr[1]=firstPart;
			}
			
			return retArr;
		}else{
			if(CommonUtil.isNotDictJson(line)) {
				retArr[0]=config.defaultTarget;
				retArr[1]=line;
				return retArr;
			}
			
			HashMap<String,Object> recordMap=CommonUtil.jsonStrToJava(line, HashMap.class);
			String target=(String)recordMap.remove(config.targetField);
			String record=CommonUtil.javaToJsonStr(recordMap);
			if(null==target || target.trim().isEmpty()) {
				retArr[0]=config.defaultTarget;
				retArr[1]=record.trim();
				return retArr;
			}
			
			retArr[0]=target.trim();
			retArr[1]=record.trim();
			return retArr;
		}
	}
}
