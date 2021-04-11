package com.df.plugin.sink.redis.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.df.plugin.sink.redis.dto.Host;
import com.df.plugin.sink.redis.util.RedisUtil;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Redis客户端配置
 */
@SuppressWarnings("unchecked")
public class RedisConfig {
	/**
	 * 发送器运行时路径
	 */
	public File sinkPath;
	
	/**
	 * 是否解析通道数据记录
	 */
	public boolean parse;
	
	/**
	 * 目标对象名索引
	 */
	public int targetIndex;
	
	/**
	 * 数据库索引
	 */
	public Integer dbIndex;
	
	/**
	 * Redis服务器密码
	 */
	public String passWord;
	
	/**
	 * Redis客户端配置
	 */
	public Properties config;
	
	/**
	 * Redis客户端工具
	 */
	public RedisUtil redisUtil;
	
	/**
	 * 目标对象名字段
	 */
	public String targetField;
	
	/**
	 * 最大重定向次数
	 */
	public Integer maxRedirect;
	
	/**
	 * 默认目标对象
	 * all:Redis全局字典或默认变量名为all的管道List
	 */
	public String defaultTarget;
	
	/**
	 * 目标架构类型
	 */
	public RedisArch targetArch;
	
	/**
	 * 目标数据对象类型
	 */
	public RedisType targetType;
	
	/**
	 * 记录字段默认分隔符
	 * 默认为英文逗号
	 */
	public String fieldSeparator;
	
	/**
	 * 记录字段默认分隔符正则式
	 * 默认为英文逗号
	 */
	public Pattern fieldSeparatorRegex;
	
	/**
	 * 发送失败后最大等待时间间隔
	 */
	public Long failMaxWaitMills;
	
	/**
	 * 发送失败后最大重试次数
	 */
	public Integer maxRetryTimes;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * Redis主机列表
	 */
	private ArrayList<Host> hostList=new ArrayList<Host>();
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
     * 数字正则式
     */
	private static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(RedisConfig.class);
	
	/**
	 * 默认序列化器
	 */
	private RedisSerializer<String> defaultSerializer=new StringRedisSerializer();
	
	/**
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	/**
	 * 值序列化器
	 */
	private RedisSerializer<Object> valueSerializer=new GenericJackson2JsonRedisSerializer();
	
	public RedisConfig(){}
	
	public RedisConfig(Flow flow) {
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * @param config
	 */
	public RedisConfig config() {
		//静态初始化Redis模板工具
		initHostAddress();
		if(hostList.isEmpty()) {
			log.error("hostList parameter must be specify...");
			throw new RuntimeException("hostList parameter must be specify...");
		}
		
		String passWordStr=config.getProperty("passWord","").trim();
		this.passWord=passWordStr.isEmpty()?null:passWordStr;
		
		String dbIndexStr=config.getProperty("dbIndex","").trim();
		this.dbIndex=dbIndexStr.isEmpty()?0:Integer.parseInt(dbIndexStr);
		
		String defaultTargetStr=config.getProperty("defaultTarget","").trim();
		this.defaultTarget=defaultTargetStr.isEmpty()?"all":defaultTargetStr;
		
		String targetTypeStr=config.getProperty("targetType","").trim();
		this.targetType=targetTypeStr.isEmpty()?RedisType.dict:RedisType.valueOf(targetTypeStr);
		
		String targetArchStr=config.getProperty("targetArch","").trim();
		this.targetArch=targetArchStr.isEmpty()?RedisArch.cluster:RedisArch.valueOf(targetArchStr);
		
		String maxRedirectStr=config.getProperty("maxRedirect","").trim();
		this.maxRedirect=maxRedirectStr.isEmpty()?(hostList.size()-1):Integer.parseInt(maxRedirectStr);
		
		this.redisUtil=new RedisUtil(getRedisTemplate());
		
		//运行时参数初始化
		String parseStr=config.getProperty("parse","").trim();
		this.parse=parseStr.isEmpty()?true:Boolean.parseBoolean(parseStr);
		
		String targetFieldStr=config.getProperty("targetField","").trim();
		this.targetField=targetFieldStr.isEmpty()?"targetObject":targetFieldStr;
		
		String fieldSeparatorStr=config.getProperty("fieldSeparator","").trim();
		this.fieldSeparator=fieldSeparatorStr.isEmpty()?"#":fieldSeparatorStr;
		this.fieldSeparatorRegex=Pattern.compile(this.fieldSeparator);
		
		String targetIndexStr=config.getProperty("targetIndex","").trim();
		this.targetIndex=targetIndexStr.isEmpty()?0:Integer.parseInt(targetIndexStr);
		
		String maxRetryTimeStr=config.getProperty("maxRetryTimes","").trim();
		this.maxRetryTimes=maxRetryTimeStr.isEmpty()?3:Integer.parseInt(maxRetryTimeStr);
		
		String failMaxTimeMillStr=config.getProperty("failMaxTimeMills","").trim();
		this.failMaxWaitMills=failMaxTimeMillStr.isEmpty()?2000:Long.parseLong(failMaxTimeMillStr);
		
		return this;
	}
	
	/**
	 * 获取Spring-Redis模板工具
	 * @return Redis模板工具
	 */
	private RedisTemplate<String,Object> getRedisTemplate() {
		RedisTemplate<String, Object> redisTemplate=new RedisTemplate<String, Object>();
		RedisConnectionFactory connectionFactory=getRedisConnectionFactory();
		redisTemplate.setConnectionFactory(connectionFactory);
		redisTemplate.setKeySerializer(defaultSerializer);
		redisTemplate.setValueSerializer(valueSerializer);
		redisTemplate.setDefaultSerializer(defaultSerializer);
		redisTemplate.setHashKeySerializer(defaultSerializer);
		redisTemplate.setHashValueSerializer(valueSerializer);
		redisTemplate.afterPropertiesSet();
		return redisTemplate;
	}
	
	/**
	 * 获取Redis连接工厂
	 * @return Redis连接工厂
	 */
	private RedisConnectionFactory getRedisConnectionFactory() {
		JedisConnectionFactory connectionFactory=null;
		if(RedisArch.single==targetArch) {
			RedisStandaloneConfiguration connectConfig = new RedisStandaloneConfiguration();
			connectionFactory = new JedisConnectionFactory(connectConfig);
			if(null!=passWord) connectConfig.setPassword(passWord);
			connectConfig.setHostName(hostList.get(0).host);
			connectConfig.setPort(hostList.get(0).port);
			connectConfig.setDatabase(dbIndex);
		}else if(RedisArch.cluster==targetArch){
			RedisClusterConfiguration connectConfig = new RedisClusterConfiguration();
			 for(Host host:hostList) connectConfig.clusterNode(host.host,host.port);
			connectionFactory=new JedisConnectionFactory(connectConfig);
			 if(null!=passWord) connectConfig.setPassword(passWord);
			 connectConfig.setMaxRedirects(maxRedirect);
		}else{
			connectionFactory=null;
		}
		
		if(null!=connectionFactory) connectionFactory.afterPropertiesSet();
		return connectionFactory;
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress(){
		String hostListStr=config.getProperty("hostList","").trim();
		String[] hosts=hostListStr.isEmpty()?new String[]{"127.0.0.1:6379"}:COMMA_REGEX.split(hostListStr);
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(host.isEmpty()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2) {
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList.add(new Host(ip,Integer.parseInt(port)));
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList.add(new Host("127.0.0.1",Integer.parseInt(unknow)));
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList.add(new Host(unknow,6379));
			}
		}
	}
	
	/**
	 * 获取字段值
	 * @param key 键
	 * @return 返回字段值
	 */
	public Object getFieldValue(String key) {
		if(null==key) return null;
		String attrName=key.trim();
		if(attrName.isEmpty()) return null;
		
		try {
			Field field=RedisConfig.class.getDeclaredField(attrName);
			field.setAccessible(true);
			
			Object fieldVal=field.get(this);
			Class<?> fieldType=field.getType();
			if(!fieldType.isArray()) return fieldVal;
			
			int len=Array.getLength(fieldVal);
			StringBuilder builder=new StringBuilder("[");
			for(int i=0;i<len;builder.append(Array.get(fieldVal, i++)).append(","));
			if(builder.length()>1) builder.deleteCharAt(builder.length()-1);
			builder.append("]");
			return builder.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 设置字段值
	 * @param key 键
	 * @param value 值
	 * @return 返回设置后的值
	 */
	public Object setFieldValue(String key,Object value) {
		if(null==key || null==value) return null;
		String attrName=key.trim();
		if(attrName.isEmpty()) return null;
		
		try {
			Field field=RedisConfig.class.getDeclaredField(attrName);
			field.setAccessible(true);
			
			Class<?> fieldType=field.getType();
			Object fieldVal=null;
			if(String[].class==fieldType){
				fieldVal=COMMA_REGEX.split(value.toString());
			}else if(File.class==fieldType){
				fieldVal=new File(value.toString());
			}else if(CommonUtil.isSimpleType(fieldType)){
				fieldVal=CommonUtil.transferType(value, fieldType);
			}else{
				return null;
			}
			
			field.set(this, fieldVal);
			return value;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 收集实时参数
	 * @return
	 */
	public String collectRealtimeParams() {
		HashMap<String,Object> map=new HashMap<String,Object>();
		map.put("parse", parse);
		map.put("hostList", hostList);
		map.put("dbIndex", dbIndex);
		map.put("sinkPath", sinkPath);
		map.put("passWord", passWord);
		map.put("targetField", targetField);
		map.put("targetArch", targetArch);
		map.put("targetType", targetType);
		map.put("targetIndex", targetIndex);
		map.put("maxRedirect", maxRedirect);
		map.put("defaultTarget", defaultTarget);
		map.put("fieldSeparator", fieldSeparator);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("preFailSinkSetSize", preFailSinkSet.size());
		return map.toString();
	}
}
