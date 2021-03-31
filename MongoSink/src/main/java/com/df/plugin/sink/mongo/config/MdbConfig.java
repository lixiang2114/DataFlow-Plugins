package com.df.plugin.sink.mongo.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.bson.Document;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.connection.ServerAddressHelper;

/**
 * @author Lixiang
 * @description MDB客户端配置
 */
@SuppressWarnings("unchecked")
public class MdbConfig {
	/**
	 * 处理记录中的ID字段
	 */
	public  String idField;
	
	/**
	 * 处理记录中的库名字段
	 */
	public String dbField;
	
	/**
	 * 处理记录中的表名字段
	 */
	public String tabField;
	
	/**
	 * 发送器运行时路径
	 */
	public File sinkPath;
	
	/**
	 * 是否解析通道数据记录
	 */
	public boolean parse;
	
	/**
	 * 登录MongoDB用户名
	 */
	public String userName;
	
	/**
	 * 登录MongoDB密码
	 */
	public String passWord;
	
	/**
	 * 默认数据库名
	 */
	public String defaultDB;
	
	/**
	 * 默认数据表名
	 */
	public String defaultTab;
	
	/**
	 * 记录字段列表
	 * 按记录行从左到右区分顺序
	 */
	public String[] fieldList;
	
	/**
	 * 批处理尺寸
	 */
	public Integer batchSize;
	
	/**
	 * MDB客户端配置
	 */
	public Properties config;
	
	/**
	 * GMT时差毫秒数
	 */
	public long timeZoneMillis;
	
	/**
	 * 记录字段默认分隔符为中英文空白正则式
	 */
	public Pattern fieldSeparator;
	
	/**
	 * 发送失败后最大等待时间间隔
	 */
	public Long failMaxWaitMills;
	
	/**
	 * 发送失败后最大重试次数
	 */
	public Integer maxRetryTimes;
	
	/**
	 * 批处理最大等待时间间隔
	 */
	public Long batchMaxWaitMills;
	
	/**
	 * MongoDB客户端
	 */
	public MongoClient mongoClient;
	
	/**
	 * 写入MDB数据库的时间字段集
	 * 通常为java.util.Date的子类型
	 */
	public Set<String> timeFieldSet;
	
	/**
	 * 写入MDB数据库的数字字段集
	 * 通常为java.lang.Number的子类型
	 */
	public Set<String> numFieldSet;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * 默认数据库(未配置默认为test)
	 */
	public MongoDatabase defaultDatabase;
	
	/**
	 * MongoDB客户端连接参数
	 */
	public MongoClientOptions mongoClientOptions;
	
	/**
	 * 默认集合表(未配置默认为test)
	 */
	public MongoCollection<Document> defaultCollection;
	
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
	 * 主机地址表
	 */
	public ArrayList<ServerAddress> hostList=new ArrayList<ServerAddress>();
	
	/**
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	public MdbConfig(){}
	
	public MdbConfig(Flow flow) {
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * @param config
	 */
	public MdbConfig config() {
		String defaultDBStr=config.getProperty("defaultDB","").trim();
		this.defaultDB=defaultDBStr.isEmpty()?null:defaultDBStr;
		
		String defaultTabStr=config.getProperty("defaultTab","").trim();
		this.defaultTab=defaultTabStr.isEmpty()?null:defaultTabStr;
		
		String dbFieldStr=config.getProperty("dbField","").trim();
		this.dbField=dbFieldStr.isEmpty()?null:dbFieldStr;
		
		String tabFieldStr=config.getProperty("tabField","").trim();
		this.tabField=tabFieldStr.isEmpty()?null:tabFieldStr;
		
		String timeZoneStr=config.getProperty("timeZone","").trim();
		this.timeZoneMillis=timeZoneStr.isEmpty()?8*3600*1000:Integer.parseInt(timeZoneStr)*36000;
		
		String numFieldStr=config.getProperty("numFields","").trim();
		this.numFieldSet=numFieldStr.isEmpty()?new HashSet<String>():Arrays.stream(COMMA_REGEX.split(numFieldStr.trim())).map(e->e.trim()).collect(Collectors.toSet());
		
		String timeFieldStr=config.getProperty("timeFields","").trim();
		this.timeFieldSet=timeFieldStr.isEmpty()?new HashSet<String>():Arrays.stream(COMMA_REGEX.split(timeFieldStr.trim())).map(e->e.trim()).collect(Collectors.toSet());
		
		initHostAddress();
		initMongoClientOptions();
		
		String parseStr=config.getProperty("parse", "").trim();
		this.parse=parseStr.isEmpty()?true:Boolean.parseBoolean(parseStr);
		
		String fieldSeparatorStr=config.getProperty("fieldSeparator","").trim();
		this.fieldSeparator=Pattern.compile(fieldSeparatorStr.isEmpty()?"\\s+":fieldSeparatorStr);
		
		String maxRetryTimeStr=config.getProperty("maxRetryTimes","").trim();
		this.maxRetryTimes=maxRetryTimeStr.isEmpty()?3:Integer.parseInt(maxRetryTimeStr);
		
		String failMaxTimeMillStr=config.getProperty("failMaxTimeMills","").trim();
		this.failMaxWaitMills=failMaxTimeMillStr.isEmpty()?2000:Long.parseLong(failMaxTimeMillStr);
		
		String batchMaxTimeMillStr=config.getProperty("batchMaxTimeMills","").trim();
		this.batchMaxWaitMills=batchMaxTimeMillStr.isEmpty()?2000:Long.parseLong(batchMaxTimeMillStr);
		
		String batchSizeStr=config.getProperty("batchSize","").trim();
		if(!batchSizeStr.isEmpty()) this.batchSize=Integer.parseInt(batchSizeStr);
		
		String idFieldStr=config.getProperty("idField","").trim();
		if(!idFieldStr.isEmpty()) {
			this.idField=idFieldStr;
		}
		
		String fieldListStr=config.getProperty("fieldList","").trim();
		if(!fieldListStr.isEmpty()){
			String[] fields=COMMA_REGEX.split(fieldListStr);
			fieldList=new String[fields.length];
			for(int i=0;i<fields.length;i++){
				String fieldName=fields[i].trim();
				if(fieldName.isEmpty()){
					fieldList[i]="field"+i;
					continue;
				}
				fieldList[i]=fieldName;
			}
		}
		
		String passWordStr=config.getProperty("passWord","").trim();
		String userNameStr=config.getProperty("userName","").trim();
		if(!passWordStr.isEmpty() && !userNameStr.isEmpty()) {
			userName=userNameStr;
			passWord=passWordStr;
		}
		
		if(null==userName || null==passWord) {
			mongoClient=new MongoClient(hostList,mongoClientOptions);
		}else{
			String loginDB=null==defaultDB?"admin":defaultDB;
			mongoClient=new MongoClient(hostList,MongoCredential.createCredential(userName, loginDB, passWord.toCharArray()),mongoClientOptions);
		}
		
		defaultDatabase=mongoClient.getDatabase(null==defaultDB?"test":defaultDB);
		defaultCollection=defaultDatabase.getCollection(null==defaultTab?"test":defaultTab,Document.class);
		
		return this;
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress(){
		String hostListStr=config.getProperty("hostList","").trim();
		String[] hosts=hostListStr.isEmpty()?new String[]{"127.0.0.1:27017"}:COMMA_REGEX.split(hostListStr);
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(0==host.length()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2){
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList.add(ServerAddressHelper.createServerAddress(ip, Integer.parseInt(port)));
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList.add(ServerAddressHelper.createServerAddress("127.0.0.1", Integer.parseInt(unknow)));
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList.add(ServerAddressHelper.createServerAddress(unknow, 27017));
			}
		}
	}
	
	/**
	 * 初始化MongoDB连接参数
	 * @param context 插件配置上下文
	 */
	private void initMongoClientOptions() {
		Builder options = MongoClientOptions.builder();
		String socketTimeout=config.getProperty("socketTimeout","").trim();
		options.socketTimeout(socketTimeout.isEmpty()?0:Integer.parseInt(socketTimeout));
		
		String maxWaitTime=config.getProperty("maxWaitTime","").trim();
		options.maxWaitTime(maxWaitTime.isEmpty()?5000:Integer.parseInt(maxWaitTime));
		
		String connectTimeout=config.getProperty("connectTimeout","").trim();
		options.connectTimeout(connectTimeout.isEmpty()?30000:Integer.parseInt(connectTimeout));
		
		String connectionsPerHost=config.getProperty("connectionsPerHost","").trim();
		options.connectionsPerHost(connectionsPerHost.isEmpty()?300:Integer.parseInt(connectionsPerHost));
		
		String cursorFinalizerEnabled=config.getProperty("cursorFinalizerEnabled","").trim();
		options.cursorFinalizerEnabled(cursorFinalizerEnabled.isEmpty()?true:Boolean.parseBoolean(cursorFinalizerEnabled));
		
		WriteConcern writeConcern=WriteConcern.UNACKNOWLEDGED;
		
		try{
			String w=config.getProperty("w","").trim();
			if(!w.isEmpty()) writeConcern=writeConcern.withW(w);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		try{
			String wTimeoutMS=config.getProperty("wTimeoutMS","").trim();
			if(!wTimeoutMS.isEmpty()) writeConcern=writeConcern.withWTimeout(Long.parseLong(wTimeoutMS), TimeUnit.MILLISECONDS);
		}catch(Exception e){
			e.printStackTrace();
		}

		try{
			String journal=config.getProperty("journal","").trim();
			if(!journal.isEmpty()) writeConcern=writeConcern.withJournal(Boolean.parseBoolean(journal));
		}catch(Exception e){
			e.printStackTrace();
		}
		
		options.writeConcern(writeConcern);
		mongoClientOptions=options.build();
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
			Field field=MdbConfig.class.getDeclaredField(attrName);
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
			Field field=MdbConfig.class.getDeclaredField(attrName);
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
		map.put("idField", idField);
		map.put("dbField", dbField);
		map.put("fieldList", fieldList);
		map.put("hostList", hostList);
		map.put("tabField", tabField);
		map.put("sinkPath", sinkPath);
		map.put("batchSize", batchSize);
		map.put("defaultDB", defaultDB);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("defaultTab", defaultTab);
		map.put("fieldSeparator", fieldSeparator);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("defaultDatabase", defaultDatabase);
		map.put("defaultCollection", defaultCollection);
		map.put("batchMaxWaitMills", batchMaxWaitMills);
		map.put("preFailSinkSetSize", preFailSinkSet.size());
		map.put("maxWaitTime", mongoClientOptions.getMaxWaitTime());
		map.put("socketTimeout", mongoClientOptions.getSocketTimeout());
		map.put("connectTimeout", mongoClientOptions.getConnectTimeout());
		map.put("connectionsPerHost", mongoClientOptions.getConnectionsPerHost());
		map.put("cursorFinalizerEnabled", mongoClientOptions.isCursorFinalizerEnabled());
		return map.toString();
	}
}
