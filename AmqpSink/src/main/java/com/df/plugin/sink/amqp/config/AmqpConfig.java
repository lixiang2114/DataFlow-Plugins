package com.df.plugin.sink.amqp.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;

import com.df.plugin.sink.amqp.dto.Route;
import com.df.plugin.sink.amqp.util.AmqpUtil;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description AMQP客户端配置
 */
@SuppressWarnings("unchecked")
public class AmqpConfig {
	/**
	 * 发送器运行时路径
	 */
	public File sinkPath;
	
	/**
	 * AMQP例程虚拟主机
	 */
	public String virHost;
	
	/**
	 * 是否解析通道数据记录
	 */
	public boolean parse;
	
	/**
	 * AMQP服务器登录密码
	 */
	public String passWord;
	
	/**
	 * AMQP服务器登录用户
	 */
	public String userName;
	
	/**
	 * AMQP客户端配置
	 */
	public Properties config;
	
	/**
	 * AMQP客户端操作工具
	 */
	public AmqpUtil amqpUtil;
	
	/**
	 * 目标交换器索引
	 */
	public int exchangeIndex;
	
	/**
	 * 目标路由键索引
	 */
	public int routingKeyIndex;
	
	/**
	 * 记录字段默认分隔符正则式
	 * 默认为英文逗号
	 */
	public Pattern fieldSeparator;
	
	/**
	 * 目标交换器字段
	 */
	public String exchangeField;
	
	/**
	 * 目标路由键字段
	 */
	public String routingKeyField;
	
	/**
	 * 默认交换器
	 */
	public String defaultExchange;
	
	/**
	 * 默认路由键
	 */
	public String defaultRoutingKey;
	
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
	private ArrayList<String> hostList=new ArrayList<String>();
	
	/**
	 * AMQP路由表
	 * example:
	 * queue1:queue;exchange1:direct;routingKey1,queue2:queue;exchange2:direct;routingKey2
	 */
	public ArrayList<Route> routeList=new ArrayList<Route>();
	
	/**
	 * 英文分号正则式
	 */
	private static final Pattern SEMIC_REGEX=Pattern.compile(";");
	
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
	private static final Logger log=LoggerFactory.getLogger(AmqpConfig.class);
	
	/**
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	/**
	 * 源组件类型
	 */
	private static HashSet<String> srcType=new HashSet<String>(Arrays.asList("direct","fanout","topic"));
	
	/**
	 * 目标组件类型
	 */
	private static HashSet<String> dstType=new HashSet<String>(Arrays.asList("queue","direct","fanout","topic"));
	
	public AmqpConfig(){}
	
	public AmqpConfig(Flow flow) {
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * AMQP配置
	 * @param config
	 */
	public AmqpConfig config() {
		//静态初始化Redis模板工具
		initHostAddress();
		if(hostList.isEmpty()) {
			log.error("hostList parameter must be specify...");
			throw new RuntimeException("hostList parameter must be specify...");
		}
		
		String virHostStr=config.getProperty("virHost","").trim();
		this.virHost=virHostStr.isEmpty()?"/":virHostStr;
		
		String passWordStr=config.getProperty("passWord","").trim();
		this.passWord=passWordStr.isEmpty()?null:passWordStr;
		
		String userNameStr=config.getProperty("userName","").trim();
		this.userName=userNameStr.isEmpty()?null:userNameStr;
		
		this.amqpUtil=new AmqpUtil(CommonUtil.joinToString(hostList, ","),virHost,userName,passWord);
		
		//运行时参数初始化
		String parseStr=config.getProperty("parse","").trim();
		this.parse=parseStr.isEmpty()?true:Boolean.parseBoolean(parseStr);
		
		String fieldSeparatorStr=config.getProperty("fieldSeparator","").trim();
		this.fieldSeparator=Pattern.compile(fieldSeparatorStr.isEmpty()?"#":fieldSeparatorStr);
		
		String maxRetryTimeStr=config.getProperty("maxRetryTimes","").trim();
		this.maxRetryTimes=maxRetryTimeStr.isEmpty()?3:Integer.parseInt(maxRetryTimeStr);
		
		String exchangeFieldStr=config.getProperty("exchangeField","").trim();
		this.exchangeField=exchangeFieldStr.isEmpty()?"exchangeName":exchangeFieldStr;
		
		String exchangeIndexStr=config.getProperty("exchangeIndex","").trim();
		this.exchangeIndex=exchangeIndexStr.isEmpty()?0:Integer.parseInt(exchangeIndexStr);
		
		String routingKeyFieldStr=config.getProperty("routingKeyField","").trim();
		this.routingKeyField=routingKeyFieldStr.isEmpty()?"routingKeyName":routingKeyFieldStr;
		
		String routingKeyIndexStr=config.getProperty("routingKeyIndex","").trim();
		this.routingKeyIndex=routingKeyIndexStr.isEmpty()?1:Integer.parseInt(routingKeyIndexStr);
		
		String failMaxTimeMillStr=config.getProperty("failMaxTimeMills","").trim();
		this.failMaxWaitMills=failMaxTimeMillStr.isEmpty()?2000:Long.parseLong(failMaxTimeMillStr);
		
		String defaultExchangeStr=config.getProperty("defaultExchange","").trim();
		this.defaultExchange=defaultExchangeStr.isEmpty()?"defaultExchange":defaultExchangeStr;
		
		String defaultRoutingKeyStr=config.getProperty("defaultRoutingKey","").trim();
		this.defaultRoutingKey=defaultRoutingKeyStr.isEmpty()?"defaultRoutingKey":defaultRoutingKeyStr;
		
		if(exchangeIndex<0 || routingKeyIndex<0 || exchangeIndex==routingKeyIndex) {
			log.error("exchangeIndex or routingKeyIndex must be greater than 0 and can not be equals...");
			throw new RuntimeException("exchangeIndex or routingKeyIndex must be greater than 0 and can not be equals...");
		}
		
		//路由拓扑架构初始化
		log.info("build routing topology architecture...");
		buildRoutingTopology();
		
		return this;
	}
	
	/**
	 * 构建RMQ路由拓扑架构
	 */
	private void buildRoutingTopology() {
		String routeBinds=config.getProperty("routeBinds","").trim();
		if(routeBinds.isEmpty()) {
			log.error("routeBinds parameter is empty...");
			throw new RuntimeException("routeBinds parameter is empty...");
		}
		
		String[] routeArray=COMMA_REGEX.split(routeBinds);
		for(String route:routeArray) {
			String[] compArray=SEMIC_REGEX.split(route.trim());
			if(2>compArray.length) {
				log.error("component number less than 2...");
				throw new RuntimeException("component number less than 2...");
			}
			
			String dstStr=compArray[0].trim();
			String srcStr=compArray[1].trim();
			if(dstStr.isEmpty() || srcStr.isEmpty()) {
				log.error("source and desting component can not be empty...");
				throw new RuntimeException("source and desting component can not be empty...");
			}
			
			String[] dstArray=COLON_REGEX.split(dstStr);
			String[] srcArray=COLON_REGEX.split(srcStr);
			if(2>dstArray.length || 2>srcArray.length) {
				log.error("incomplete component description for source or desting...");
				throw new RuntimeException("incomplete component description for source or desting...");
			}
			
			String dstName=dstArray[0].trim();
			String dstType=dstArray[1].trim();
			String srcName=srcArray[0].trim();
			String srcType=srcArray[1].trim();
			if(dstName.isEmpty() || dstType.isEmpty() || srcName.isEmpty() || srcType.isEmpty()) {
				log.error("name and type can not be empty for source or desting...");
				throw new RuntimeException("name and type can not be empty for source or desting...");
			}
			
			if(!AmqpConfig.srcType.contains(srcType) || !AmqpConfig.dstType.contains(dstType)) {
				log.error("srcType: {} or dstType: {} is not defined...",srcType,dstType);
				throw new RuntimeException("srcType: "+srcType+" or dstType: "+dstType+" is not defined...");
			}
			
			String routingKey=2<compArray.length?compArray[2].trim():"";
			if(!"fanout".equals(srcType) && routingKey.isEmpty()) {
				log.error("routingKey must be specified when non-fanout mode...");
				throw new RuntimeException("routingKey must be specified when non-fanout mode...");
			}
			
			Binding binding=amqpUtil.addBinding(dstName, dstType, srcName, srcType, routingKey);
			routeList.add(new Route(binding,dstName,dstType,srcName,srcType,routingKey));
			log.info("{}",binding);
		}
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress() {
		String hostListStr=config.getProperty("hostList","").trim();
		String[] hosts=hostListStr.isEmpty()?new String[]{"127.0.0.1:5672"}:COMMA_REGEX.split(hostListStr);
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(host.isEmpty()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2) {
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList.add(ip+":"+port);
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList.add("127.0.0.1:"+unknow);
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList.add(unknow+":5672");
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
			Field field=AmqpConfig.class.getDeclaredField(attrName);
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
			Field field=AmqpConfig.class.getDeclaredField(attrName);
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
		map.put("virHost", virHost);
		map.put("hostList", hostList);
		map.put("sinkPath", sinkPath);
		map.put("routeList", routeList);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("fieldSeparator", fieldSeparator);
		map.put("exchangeField", exchangeField);
		map.put("exchangeIndex", exchangeIndex);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("routingKeyField", routingKeyField);
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("routingKeyIndex", routingKeyIndex);
		map.put("defaultExchange", defaultExchange);
		map.put("defaultRoutingKey", defaultRoutingKey);
		map.put("preFailSinkSetSize", preFailSinkSet.size());
		return map.toString();
	}
}
