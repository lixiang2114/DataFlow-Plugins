package com.df.plugin.sink.redis.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
				isSuc=sendMap(redisMapper.target,(HashMap<String,Object>)redisMapper.message);
			}else{
				String target=redisMapper.target;
				List<HashMap<String, Object>> msgList=(List<HashMap<String, Object>>)redisMapper.message;
				
				int index=-1;
				int len=msgList.size();
				for(int i=0;i<len;i++) {
					if(!sendRecord(target, msgList.get(i))) {
						break;
					}
					index=i;
				}
				
				if(index+1==len) {
					isSuc=true;
				}else{
					isSuc=false;
					redisMapper.message=msgList.subList(index+1, len);
				}
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
		
		HashMap<String,Object> messageMap=CommonUtil.jsonStrToJava(message, HashMap.class);
		if(null==messageMap || messageMap.isEmpty()) {
			log.error("dict message: {} transfer to messageMap is null or empty...",message);
			return true;
		}
		
		ArrayList<String> sucKeys=new ArrayList<String>();
		if("all".equals(target)) {
			for(Entry<String, Object> entry:messageMap.entrySet()) {
				String key=entry.getKey();
				if(!sendKeyVal(key,entry.getValue())) break;
				sucKeys.add(key);
			}
		}else{
			if(sendMap(target, messageMap)) sucKeys.addAll(messageMap.keySet());
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
	private boolean sendKeyVal(String target,Object message) throws Exception{
		boolean loop=false;
		int times=0;
		do{
			try{
				RedisUtil.set(target, message);
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
	private boolean sendMap(String target,HashMap<String,Object> messageMap) throws Exception{
		boolean loop=false;
		int times=0;
		do{
			try{
				RedisUtil.hashSet(target, messageMap);
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
		
		List<HashMap<String,Object>> msgList=new ArrayList<HashMap<String,Object>>();
		String[] messages=redisConfig.fieldSeparatorRegex.split(message);
		try{
			for(int i=0;i<messages.length;msgList.add(CommonUtil.jsonStrToJava(messages[i++], HashMap.class)));
		}catch(Exception e) {
			log.error("pipe messages transfer to messageMap occur exception...",Arrays.toString(messages));
		}
		
		if(null==msgList || msgList.isEmpty()) {
			log.error("pipe messages transfer to messageMap is null or empty...");
			return true;
		}
		
		int index=-1;
		int len=msgList.size();
		for(int i=0;i<len;i++) {
			if(!sendRecord(target, msgList.get(i))) break;
			index=i;
		}
		
		if(len==index+1) return true;
		redisConfig.preFailSinkSet.add(new RedisMapper(target,msgList.subList(index+1, len),redisConfig.targetType));
		
		return false;
	}
	
	/**
	 * 发送单个记录
	 * @param target 目标对象
	 * @param message 消息内容
	 * @return 是否发送成功
	 * @throws Exception
	 */
	private boolean sendRecord(String target,Object message) throws Exception{
		boolean loop=false;
		int times=0;
		do{
			try{
				RedisUtil.leftPush(target, message);
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
