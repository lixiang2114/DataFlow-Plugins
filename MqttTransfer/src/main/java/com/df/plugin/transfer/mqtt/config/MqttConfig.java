package com.df.plugin.transfer.mqtt.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLSocketFactory;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.mqtt.handler.MqttHandler;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.SizeUnit;
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
	 * 插件运行时路径
	 */
	public File pluginPath;
	
	/**
	 * 转存目录
	 */
	public File transferPath;
	
	/**
	 * 实时转存的日志文件
	 */
	public File transferSaveFile;
	
	/**
	 * 转存日志文件最大尺寸
	 */
	public Long transferSaveMaxSize;
	
	/**
	 * 通信质量指标
	 */
	private Integer qos;
	
	/**
	 * 登录Mqtt服务的Token
	 */
	private Token token;
	
	/**
	 * 连接主题名称
	 */
	private String topic;
	
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
	 * Mqtt客户端断开连接后是否需要Mqtt服务端仍然保持会话数据
	 */
	private Boolean cleanSession;
	
	/**
	 * Mqtt客户端与Mqtt服务端保持长连接的心跳间隔时间
	 */
	private Integer keepAliveInterval;
	
	/**
	 * Mqtt客户端连接Mqtt服务端的超时时间(下发超时时间)
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
	 * Mqtt客户端断开连接后是否需要自动重连到Mqtt服务端
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
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	public MqttConfig(){}
	
	public MqttConfig(Flow flow){
		this.transferPath=flow.sharePath;
		this.pluginPath=flow.transferPath;
		this.config=PropertiesReader.getProperties(new File(pluginPath,"transfer.properties"));
	}
	
	public Integer getQos() {
		return null==qos?1:qos;
	}
	
	public Token getToken() {
		return token;
	}
	
	public String getJwtSecret() {
		return jwtSecret;
	}
	
	public String[] getHostList() {
		return hostList;
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
	
	public String getTopic() {
		if(null==topic) throw new RuntimeException("ERROR: not configured for topic list...");
		return topic;
	}
	
	/**
	 * @param config
	 */
	public MqttConfig config() {
		//配置转存文件参数
		String transferSaveFileName=config.getProperty("transferSaveFile","").trim();
		if(transferSaveFileName.isEmpty()) {
			this.transferSaveFile=new File(transferPath,"buffer.log.0");
			log.warn("not found parameter: 'transferSaveFile',will be use default...");
		}else{
			File file=new File(transferSaveFileName);
			if(!file.exists() || file.isFile()) transferSaveFile=file;
		}
		
		if(null==transferSaveFile) {
			log.error("transferSaveFile can not be NULL...");
			throw new RuntimeException("transferSaveFile can not be NULL...");
		}
		
		log.info("transfer save logger file is: "+transferSaveFile.getAbsolutePath());
		
		this.transferSaveMaxSize=getTransferSaveMaxSize();
		log.info("transfer save logger file max size is: "+transferSaveMaxSize);
		
		//配置MQTT连接参数
		String qosStr=config.getProperty("qos","").trim();
		String topicStr=config.getProperty("topic","").trim();
		String hostStr=config.getProperty("hostList","").trim();
		String retainedStr=config.getProperty("retained","").trim();
		String jwtSecretStr=config.getProperty("jwtSecret","").trim();
		String passWordStr=config.getProperty("passWord","").trim();
		String rootCaFileStr=config.getProperty("rootCaFile","").trim();
		String userNameStr=config.getProperty("userName","").trim();
		String protocolStr=config.getProperty("protocolType","").trim();
		String tokenFromStr=config.getProperty("tokenFrom","").trim();
		String maxInflightStr=config.getProperty("maxInflight","").trim();
		String clientCaFileStr=config.getProperty("clientCaFile","").trim();
		String tokenExpireStr=config.getProperty("tokenExpire","").trim();
		String expireFactorStr=config.getProperty("expireFactor","").trim();
		String clientKeyFileStr=config.getProperty("clientKeyFile","").trim();
		String cleanSessionStr=config.getProperty("cleanSession","").trim();
		String persistenceStr=config.getProperty("persistenceType","").trim();
		String keepAliveIntervalStr=config.getProperty("keepAliveInterval","").trim();
		String clientCaPasswordStr=config.getProperty("clientCaPassword","").trim();
		String connectionTimeoutStr=config.getProperty("connectionTimeout","").trim();
		String automaticReconnectStr=config.getProperty("automaticReconnect","").trim();
		
		this.topic=topicStr.isEmpty()?null:topicStr;
		this.qos=qosStr.isEmpty()?null:new Integer(qosStr);
		this.jwtSecret=jwtSecretStr.isEmpty()?null:jwtSecretStr;
		this.passWord=passWordStr.isEmpty()?null:passWordStr;
		this.protocolType=protocolStr.isEmpty()?null:protocolStr;
		this.userName=userNameStr.isEmpty()?null:userNameStr;
		this.tokenFrom=tokenFromStr.isEmpty()?null:tokenFromStr;
		this.retained=retainedStr.isEmpty()?null:Boolean.parseBoolean(retainedStr);
		this.maxInflight=maxInflightStr.isEmpty()?null:Integer.parseInt(maxInflightStr);
		this.clientCaPassword=clientCaPasswordStr.isEmpty()?null:clientCaPasswordStr;
		this.tokenExpire=tokenExpireStr.isEmpty()?null:Integer.parseInt(tokenExpireStr);
		this.expireFactor=expireFactorStr.isEmpty()?null:Integer.parseInt(expireFactorStr);
		this.rootCaFile=rootCaFileStr.isEmpty()?null:new File(pluginPath,"certs/"+rootCaFileStr);
		this.cleanSession=cleanSessionStr.isEmpty()?null:Boolean.parseBoolean(cleanSessionStr);
		this.clientCaFile=clientCaFileStr.isEmpty()?null:new File(pluginPath,"certs/"+clientCaFileStr);
		this.clientKeyFile=clientKeyFileStr.isEmpty()?null:new File(pluginPath,"certs/"+clientKeyFileStr);
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
	private void configHostAddress(String hosts) {
		String protocolType=getProtocolType()+"://";
		String defaultPort="ssl://".equals(protocolType)?DEFAULT_SSL_PORT:DEFAULT_TCP_PORT;
		String hostStr=hosts.isEmpty()?(DEFAULT_HOST+":"+defaultPort):hosts;
		
		ArrayList<String> tmpList=new ArrayList<String>();
		String[] hostArray=COMMA_REGEX.split(hostStr);
		for(int i=0;i<hostArray.length;i++){
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
			mqttClient.connectWithResult(mqttConnectOptions);
			return mqttClient;
		} catch (MqttException e) {
			log.error("connection Mqtt server occur error,cause is:",e);
			 throw new RuntimeException(e);
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
		map.put("topic", topic);
		map.put("retained", retained);
		map.put("jwtSecret", jwtSecret);
		map.put("rootCaFile", rootCaFile);
		map.put("tokenFrom", tokenFrom);
		map.put("clientCaFile", clientCaFile);
		map.put("token", token.getToken());
		map.put("tokenExpire", tokenExpire);
		map.put("transferPath", transferPath);
		map.put("expireFactor", expireFactor);
		map.put("clientKeyFile", clientKeyFile);
		map.put("protocolType", protocolType);
		map.put("tokenFromPass", tokenFromPass);
		map.put("clientId", mqttClient.getClientId());
		map.put("hostList", Arrays.toString(hostList));
		map.put("transferSaveFile", transferSaveFile);
		map.put("clientCaPassword", clientCaPassword);
		map.put("transferSaveMaxSize", transferSaveMaxSize);
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
