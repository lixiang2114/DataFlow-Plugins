package com.df.plugin.transfer.amqp.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;

import com.df.plugin.transfer.amqp.dto.Route;
import com.df.plugin.transfer.amqp.util.AmqpUtil;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description AMQP客户端配置
 */
@SuppressWarnings("unchecked")
public class AmqpConfig {
	/**
	 * 流程实例
	 */
	public Flow flow;
	
	/**
	 * AMQP例程虚拟主机
	 */
	public String virHost;
	
	/**
	 * 插件运行时路径
	 */
	public File pluginPath;
	
	/**
	 * AMQP服务器密码
	 */
	public String passWord;
	
	/**
	 * AMQP服务器用户
	 */
	public String userName;
	
	/**
	 * 转存目录
	 */
	public File transferPath;
	
	/**
	 * Redis客户端配置
	 */
	private Properties config;
	
	/**
	 * AMQP客户端操作工具
	 */
	public AmqpUtil amqpUtil;
	
	/**
	 * 实时转存的日志文件
	 */
	public File transferSaveFile;
	
	/**
	 * 拉取超时时间(单位:毫秒)
	 */
	public Long pollTimeoutMills;
	
	/**
	 * 扫描的目标队列
	 */
	public String[] targetQueues;
	
	/**
	 * 转存日志文件最大尺寸
	 */
	public Long transferSaveMaxSize;
	
	/**
	 * 当本插件退出时删除路由拓扑
	 */
	public boolean delRoutingOnExit;
	
	/**
	 * AMQP主机列表
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
	public static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(AmqpConfig.class);
	
	/**
     * IP地址正则式
     */
	public static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	/**
	 * 源组件类型
	 */
	private static HashSet<String> srcType=new HashSet<String>(Arrays.asList("direct","fanout","topic"));
	
	/**
	 * 目标组件类型
	 */
	private static HashSet<String> dstType=new HashSet<String>(Arrays.asList("queue","direct","fanout","topic"));
	
	public AmqpConfig(){}
	
	public AmqpConfig(Flow flow){
		this.flow=flow;
		this.transferPath=flow.sharePath;
		this.pluginPath=flow.transferPath;
		this.config=PropertiesReader.getProperties(new File(pluginPath,"transfer.properties"));
	}
	
	/**
	 * @param config
	 */
	public AmqpConfig config() {
		String transferSaveFileName=config.getProperty("transferSaveFile","").trim();
		if(transferSaveFileName.isEmpty()) {
			transferSaveFile=new File(transferPath,"buffer.log.0");
			log.warn("not found parameter: 'transferSaveFile',will be use default...");
		}else{
			File file=new File(transferSaveFileName);
			if(!file.exists() || file.isFile()) transferSaveFile=file;
		}
		
		if(null==transferSaveFile) {
			log.error("transferSaveFile can not be NULL...");
			throw new RuntimeException("transferSaveFile can not be NULL...");
		}
		
		log.info("transfer save file is: "+transferSaveFile.getAbsolutePath());
		
		transferSaveMaxSize=getTransferSaveMaxSize();
		log.info("transfer save file max size is: "+transferSaveMaxSize);
		
		initHostAddress();
		
		if(hostList.isEmpty()) {
			log.error("hostList parameter must be specify...");
			throw new RuntimeException("hostList parameter must be specify...");
		}
		
		String targetQueueStr=config.getProperty("targetQueues","").trim();
		if(targetQueueStr.isEmpty()) {
			log.error("targetQueues parameter must be specify...");
			throw new RuntimeException("targetQueues parameter must be specify...");
		}else{
			this.targetQueues=COMMA_REGEX.split(targetQueueStr);
			for(int i=0;i<targetQueues.length;targetQueues[i]=targetQueues[i++].trim());
		}
		
		String virHostStr=config.getProperty("virHost","").trim();
		this.virHost=virHostStr.isEmpty()?"/":virHostStr;
		
		String passWordStr=config.getProperty("passWord","").trim();
		this.passWord=passWordStr.isEmpty()?null:passWordStr;
		
		String userNameStr=config.getProperty("userName","").trim();
		this.userName=userNameStr.isEmpty()?null:userNameStr;
		
		String delRoutingOnExitStr=config.getProperty("delRoutingOnExit","").trim();
		this.delRoutingOnExit=delRoutingOnExitStr.isEmpty()?false:Boolean.parseBoolean(delRoutingOnExitStr);
		
		String pollTimeoutMillStr=config.getProperty("pollTimeoutMills","").trim();
		this.pollTimeoutMills=pollTimeoutMillStr.isEmpty()?100:Long.parseLong(pollTimeoutMillStr);
		
		this.amqpUtil=new AmqpUtil(CommonUtil.joinToString(hostList, ","),virHost,userName,passWord);
		
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
	 * 获取转存日志文件最大尺寸(默认为2GB)
	 */
	private Long getTransferSaveMaxSize(){
		String configMaxVal=config.getProperty("transferSaveMaxSize","").trim();
		if(configMaxVal.isEmpty()) return 2*1024*1024*1024L;
		Matcher matcher=CAP_REGEX.matcher(configMaxVal);
		if(!matcher.find()) return 2*1024*1024*1024L;
		return SizeUnit.getBytes(Long.parseLong(matcher.group(1)), matcher.group(2).substring(0,1));
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
		map.put("virHost", virHost);
		map.put("hostList", hostList);
		map.put("routeList", routeList);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("pluginPath", pluginPath);
		map.put("transferPath", transferPath);
		map.put("transferSaveFile", transferSaveFile);
		map.put("pollTimeoutMills", pollTimeoutMills);
		map.put("delRoutingOnExit", delRoutingOnExit);
		map.put("transferSaveMaxSize", transferSaveMaxSize);
		map.put("targetQueues", Arrays.toString(targetQueues));
		return map.toString();
	}
}
