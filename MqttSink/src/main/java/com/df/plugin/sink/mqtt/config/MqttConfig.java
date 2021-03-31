package com.df.plugin.sink.mqtt.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import javax.net.ssl.SSLSocketFactory;

import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.mqtt.handler.MqttHandler;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.Token;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description MQTT客户端配置
 */
@SuppressWarnings("unchecked")
public class MqttConfig {
	/**
	 * 发送器运行时路径
	 */
	private File sinkPath;
	
	/**
	 * 通信质量指标
	 */
	private Integer qos;
	
	/**
	 * 是否解析通道记录
	 */
	private boolean parse;
	
	/**
	 * 登录Mqtt服务的Token
	 */
	private Token token;
	
	/**
	 * 连接主题名称
	 */
	private String defaultTopic;
	
	/**
	 * Mqtt客户端配置
	 */
	private Properties config;
	
	/**
	 * 主机列表
	 */
	private String[] hostList;
	
	/**
	 * 是否设置为保留消息
	 */
	private Boolean retained;
	
	/**
	 * 登录Mqtt服务的密码
	 */
	private String passWord;
	
	/**
	 * 登录Mqtt服务的用户名
	 */
	private String userName;
	
	/**
	 * 登录验证Token的秘钥
	 */
	private String jwtSecret;
	
	/**
	 * 主题字段索引
	 * 默认值为0
	 */
	private int topicIndex=0;
	
	/**
	 * 字段分隔符
	 * 默认值为英文逗号
	 */
	private String fieldSeparator;
	
	/**
	 * 主题字段名称
	 */
	private String topicField;
	
	/**
	 * 根证书文件
	 */
	private File rootCaFile;
	
	/**
	 * 客户端证书文件
	 */
	private File clientCaFile;
	
	/**
	 * 客户端秘钥文件
	 */
	private File clientKeyFile;
	
	/**
	 * 客户端证书密码
	 */
	private String clientCaPassword;
	
	/**
	 * 携带Token的字段名
	 */
	private String tokenFrom;
	
	/**
	 * Token过期时间
	 */
	private Integer tokenExpire;
	
	/**
	 * 连接协议
	 */
	private String protocolType;
	
	/**
	 * Mqtt客户端
	 */
	public MqttClient mqttClient;
	
	/**
	 * Token过期时间因数
	 */
	private Integer expireFactor;
	
	/**
	 * 飞行队列(发送端二级队列)最大尺寸
	 */
	private Integer maxInflight;
	
	/**
	 * 发送失败后最大等待时间间隔
	 */
	public Long failMaxWaitMills;
	
	/**
	 * 发送失败后最大重试次数
	 */
	public Integer maxRetryTimes;
	
	/**
	 * MqttSink断开连接后是否需要Mqtt服务端仍然保持会话数据
	 */
	private Boolean cleanSession;
	
	/**
	 * MqttSink与Mqtt服务端保持长连接的心跳间隔时间
	 */
	private Integer keepAliveInterval;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * MqttSink连接Mqtt服务端的超时时间(下发超时时间)
	 */
	private Integer connectionTimeout;
	
	/**
	 * 是否使用密码字段携带Token
	 */
	public boolean tokenFromPass=true;
	
	/**
	 * 是否需要启动Token过期调度器
	 */
	public Boolean startTokenScheduler;
	
	/**
	 * MqttSink断开连接后是否需要自动重连到Mqtt服务端
	 */
	private Boolean automaticReconnect;
	
	/**
	 * 消息持久化类型
	 */
	private MqttClientPersistence persistenceType;
	
	/**
	 * Mqtt客户端连接参数
	 */
	public MqttConnectOptions mqttConnectOptions;
	
	/**
	 * 默认主机地址
	 */
	private static final String DEFAULT_HOST="127.0.0.1";
	
	/**
	 * 默认TCP协议端口
	 */
	private static final String DEFAULT_TCP_PORT="1883";
	
	/**
	 * 默认SSL协议端口
	 */
	private static final String DEFAULT_SSL_PORT="8883";
	
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
	private static final Logger log=LoggerFactory.getLogger(MqttConfig.class);
	
	/**
     * IP地址正则式
     */
	public static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	public MqttConfig(){}
	
	public MqttConfig(Flow flow){
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	public Integer getQos() {
		return null==qos?1:qos;
	}
	
	public boolean getParse() {
		return parse;
	}
	
	public Token getToken() {
		return token;
	}
	
	public String getJwtSecret() {
		return jwtSecret;
	}
	
	public String getTopicField(){
		return null==topicField?"topic":topicField;
	}
	
	public int getTopicIndex(){
		return topicIndex;
	}
	
	public String[] getHostList() {
		return hostList;
	}
	
	public String getFieldSeparator() {
		return null==fieldSeparator?",":fieldSeparator;
	}
	
	public String getDefaultTopic() {
		return defaultTopic;
	}
	
	public String getTokenValue() {
		try {
			return token.getToken();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Boolean getRetained() {
		return null==retained?false:retained;
	}
	
	public Integer getMaxInflight() {
		return null==maxInflight?10:maxInflight;
	}
	
	public String getUserName() {
		return null==userName?"admin":userName;
	}
	
	public String getPassWord() {
		return null==passWord?"public":passWord;
	}

	public Boolean getCleanSession() {
		return null==cleanSession?true:cleanSession;
	}
	
	public String getTokenFrom() {
		return null==tokenFrom?"password":tokenFrom;
	}
	
	public String getProtocolType() {
		return null==protocolType?"tcp":protocolType;
	}
	
	public Integer getKeepAliveInterval() {
		return null==keepAliveInterval?60:keepAliveInterval;
	}

	public Integer getConnectionTimeout() {
		return null==connectionTimeout?30:connectionTimeout;
	}
	
	public Integer getTokenExpire() {
		if(null==tokenExpire) return 3600;
		return -1==tokenExpire.intValue()?1000000000:tokenExpire;
	}

	public Boolean getAutomaticReconnect() {
		return null==automaticReconnect?true:automaticReconnect;
	}
	
	public MqttClientPersistence getPersistenceType() {
		return null==persistenceType?new MemoryPersistence():persistenceType;
	}
	
	public Integer getExpireFactor() {
		if(null==tokenExpire) return null==expireFactor?750:expireFactor;
		return -1==tokenExpire.intValue()?1000:null==expireFactor?750:expireFactor;
	}
	
	/**
	 * @param config
	 */
	public MqttConfig config() {
		String qosStr=config.getProperty("qos","").trim();
		String parseStr=config.getProperty("parse","").trim();
		String hostStr=config.getProperty("hostList","").trim();
		String retainedStr=config.getProperty("retained","").trim();
		String jwtSecretStr=config.getProperty("jwtSecret","").trim();
		String topicFieldStr=config.getProperty("topicField","").trim();
		String topicIndexStr=config.getProperty("topicIndex","").trim();
		String passWordStr=config.getProperty("passWord","").trim();
		String rootCaFileStr=config.getProperty("rootCaFile","").trim();
		String userNameStr=config.getProperty("userName","").trim();
		String protocolStr=config.getProperty("protocolType","").trim();
		String tokenFromStr=config.getProperty("tokenFrom","").trim();
		String maxInflightStr=config.getProperty("maxInflight","").trim();
		String clientCaFileStr=config.getProperty("clientCaFile","").trim();
		String tokenExpireStr=config.getProperty("tokenExpire","").trim();
		String defaultTopicStr=config.getProperty("defaultTopic","").trim();
		String expireFactorStr=config.getProperty("expireFactor","").trim();
		String clientKeyFileStr=config.getProperty("clientKeyFile","").trim();
		String cleanSessionStr=config.getProperty("cleanSession","").trim();
		String persistenceStr=config.getProperty("persistenceType","").trim();
		String fieldSeparatorStr=config.getProperty("fieldSeparator","").trim();
		String maxRetryTimesStr=config.getProperty("maxRetryTimes","").trim();
		String failMaxWaitMillStr=config.getProperty("failMaxWaitMills","").trim();
		String keepAliveIntervalStr=config.getProperty("keepAliveInterval","").trim();
		String clientCaPasswordStr=config.getProperty("clientCaPassword","").trim();
		String connectionTimeoutStr=config.getProperty("connectionTimeout","").trim();
		String automaticReconnectStr=config.getProperty("automaticReconnect","").trim();
		
		this.qos=qosStr.isEmpty()?null:new Integer(qosStr);
		this.jwtSecret=jwtSecretStr.isEmpty()?null:jwtSecretStr;
		this.topicField=topicFieldStr.isEmpty()?null:topicFieldStr;
		this.passWord=passWordStr.isEmpty()?null:passWordStr;
		this.protocolType=protocolStr.isEmpty()?null:protocolStr;
		this.userName=userNameStr.isEmpty()?null:userNameStr;
		this.tokenFrom=tokenFromStr.isEmpty()?null:tokenFromStr;
		this.defaultTopic=defaultTopicStr.isEmpty()?null:defaultTopicStr;
		this.parse=parseStr.isEmpty()?true:Boolean.parseBoolean(parseStr);
		this.fieldSeparator=fieldSeparatorStr.isEmpty()?null:fieldSeparatorStr;
		this.retained=retainedStr.isEmpty()?null:Boolean.parseBoolean(retainedStr);
		this.topicIndex=topicIndexStr.isEmpty()?0:Integer.parseInt(topicIndexStr);
		this.maxInflight=maxInflightStr.isEmpty()?null:Integer.parseInt(maxInflightStr);
		this.clientCaPassword=clientCaPasswordStr.isEmpty()?null:clientCaPasswordStr;
		this.tokenExpire=tokenExpireStr.isEmpty()?null:Integer.parseInt(tokenExpireStr);
		this.expireFactor=expireFactorStr.isEmpty()?null:Integer.parseInt(expireFactorStr);
		this.rootCaFile=rootCaFileStr.isEmpty()?null:new File(sinkPath,"certs/"+rootCaFileStr);
		this.maxRetryTimes=maxRetryTimesStr.isEmpty()?3:Integer.parseInt(maxRetryTimesStr);
		this.cleanSession=cleanSessionStr.isEmpty()?null:Boolean.parseBoolean(cleanSessionStr);
		this.clientCaFile=clientCaFileStr.isEmpty()?null:new File(sinkPath,"certs/"+clientCaFileStr);
		this.clientKeyFile=clientKeyFileStr.isEmpty()?null:new File(sinkPath,"certs/"+clientKeyFileStr);
		this.failMaxWaitMills=failMaxWaitMillStr.isEmpty()?2000L:Long.parseLong(failMaxWaitMillStr);
		this.keepAliveInterval=keepAliveIntervalStr.isEmpty()?null:Integer.parseInt(keepAliveIntervalStr);
		this.connectionTimeout=connectionTimeoutStr.isEmpty()?null:Integer.parseInt(connectionTimeoutStr);
		this.automaticReconnect=automaticReconnectStr.isEmpty()?null:Boolean.parseBoolean(automaticReconnectStr);
		try {
			this.persistenceType=persistenceStr.isEmpty()?null:(MqttClientPersistence)Class.forName(persistenceStr).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		configHostAddress(hostStr);
		configMqttClientOptions();
		
		return this;
	}
	
	/**
	 * 初始化Mqtt主机地址
	 * @param context
	 */
	private void configHostAddress(String hosts){
		String protocolType=getProtocolType()+"://";
		String defaultPort="ssl://".equals(protocolType)?DEFAULT_SSL_PORT:DEFAULT_TCP_PORT;
		String hostStr=hosts.isEmpty()?(DEFAULT_HOST+":"+defaultPort):hosts;
		
		ArrayList<String> tmpList=new ArrayList<String>();
		String[] hostArray=COMMA_REGEX.split(hostStr);
		for(int i=0;i<hostArray.length;i++) {
			String host=hostArray[i].trim();
			if(host.isEmpty()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2){
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				tmpList.add(new StringBuilder(protocolType).append(ip).append(":").append(port).toString());
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				tmpList.add(new StringBuilder(protocolType).append(DEFAULT_HOST+":").append(unknow).toString());
			}else if(IP_REGEX.matcher(unknow).matches()){
				tmpList.add(new StringBuilder(protocolType).append(unknow).append(":").append(defaultPort).toString());
			}
		}
		
		int hostCount=tmpList.size();
		if(0!=hostCount) hostList=tmpList.toArray(new String[hostCount]);
	}
	
	/**
	 * 初始化Mqtt主机连接参数
	 * @param context
	 */
	private void configMqttClientOptions() {
		mqttConnectOptions = new MqttConnectOptions();
		mqttConnectOptions.setServerURIs(hostList);
		mqttConnectOptions.setMaxInflight(getMaxInflight());
		mqttConnectOptions.setCleanSession(getCleanSession());
		mqttConnectOptions.setKeepAliveInterval(getKeepAliveInterval());
		mqttConnectOptions.setConnectionTimeout(getConnectionTimeout());
		mqttConnectOptions.setAutomaticReconnect(getAutomaticReconnect());
		
		if(null!=rootCaFile) {
			try{
				SSLSocketFactory sslSocketFactory=null;
				if(null==clientCaFile){
					sslSocketFactory=TLSConfig.getSSLSocketFactory(rootCaFile);
				}else{
					sslSocketFactory=TLSConfig.getSSLSocketFactory(rootCaFile,clientCaFile,clientKeyFile,clientCaPassword);
				}
				mqttConnectOptions.setSocketFactory(sslSocketFactory);
			}catch(Exception e){
				log.error("obtain ssl socket factory failure:",e);
				throw new RuntimeException(e);
			}
		}
		
		if(null==jwtSecret){
			mqttConnectOptions.setUserName(getUserName());
			mqttConnectOptions.setPassword(getPassWord().toCharArray());
			return;
		}
		
		if(-1==tokenExpire.intValue()) startTokenScheduler=false;
		
		try {
			token=new Token(jwtSecret, getTokenExpire(), getUserName(), getExpireFactor());
		} catch (Exception e) {
			log.error("create token instance failure:",e);
			throw new RuntimeException(e);
		}
		
		String tokenStr=null;
		try {
			tokenStr=token.getToken();
		} catch (Exception e) {
			log.error("get token value occur error: ",e);
			throw new RuntimeException("get token value occur error...");
		}
		
		if("username".equalsIgnoreCase(getTokenFrom())) {
			userName=tokenStr;
			tokenFromPass=false;
		}else {
			passWord=tokenStr;
			tokenFromPass=true;
		}
		
		if(null==startTokenScheduler) startTokenScheduler=true;
		
		mqttConnectOptions.setUserName(getUserName());
		mqttConnectOptions.setPassword(getPassWord().toCharArray());
	}
	
	/**
	 * 连接Mqtt服务端
	 */
	public MqttClient connectMqttServer() {
		try {
			mqttClient=new MqttClient(mqttConnectOptions.getServerURIs()[0],MqttClient.generateClientId(),persistenceType);
			mqttClient.setCallback(new MqttHandler(this));
			
			IMqttToken mqttToken=mqttClient.connectWithResult(mqttConnectOptions);
			mqttToken.waitForCompletion();
			return mqttClient;
		} catch (MqttException e) {
			log.error("connection Mqtt server occur error,cause is:",e);
			 throw new RuntimeException(e);
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
			Field field=MqttConfig.class.getDeclaredField(attrName);
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
			Field field=MqttConfig.class.getDeclaredField(attrName);
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
		map.put("qos", qos);
		map.put("parse", parse);
		map.put("retained", retained);
		map.put("jwtSecret", jwtSecret);
		map.put("topicField", topicField);
		map.put("topicIndex", topicIndex);
		map.put("rootCaFile", rootCaFile);
		map.put("tokenFrom", tokenFrom);
		map.put("clientCaFile", clientCaFile);
		map.put("token", token.getToken());
		map.put("tokenExpire", tokenExpire);
		map.put("defaultTopic", defaultTopic);
		map.put("expireFactor", expireFactor);
		map.put("clientKeyFile", clientKeyFile);
		map.put("protocolType", protocolType);
		map.put("fieldSeparator", fieldSeparator);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("tokenFromPass", tokenFromPass);
		map.put("clientId", mqttClient.getClientId());
		map.put("hostList", Arrays.toString(hostList));
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("clientCaPassword", clientCaPassword);
		map.put("preFailSinkSetSize", preFailSinkSet.size());
		map.put("startTokenScheduler", startTokenScheduler);
		map.put("userName", mqttConnectOptions.getUserName());
		map.put("maxInflight", mqttConnectOptions.getMaxInflight());
		map.put("persistence", persistenceType.getClass().getName());
		map.put("isCleanSession", mqttConnectOptions.isCleanSession());
		map.put("passWord", new String(mqttConnectOptions.getPassword()));
		map.put("keepAliveInterval", mqttConnectOptions.getKeepAliveInterval());
		map.put("connectionTimeout", mqttConnectOptions.getConnectionTimeout());
		map.put("automaticReconnect", mqttConnectOptions.isAutomaticReconnect());
		return map.toString();
	}
}
