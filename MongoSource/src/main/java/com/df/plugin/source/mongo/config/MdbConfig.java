package com.df.plugin.source.mongo.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;
import com.mongodb.BasicDBObject;
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
 * @description MongoDB客户端配置
 */
@SuppressWarnings("unchecked")
public class MdbConfig {
	/**
	 * 当前流程实例对象
	 */
	public Flow flow;
	
	/**
	 * 收集器运行时路径
	 */
	public File sourcePath;
	
	/**
	 * Mdb服务器用户
	 */
	public String userName;
	
	/**
	 * 是否为实时读取
	 */
	public Boolean realtime;
	
	/**
	 * Mdb服务器密码
	 */
	public String passWord;
	
	/**
	 * 拉取数据SQL语句
	 */
	private String selectSQL;
	
	/**
	 * 输出数据格式
	 * qstr: 查询字串格式 
	 * map: 字典Json格式
	 */
	public String outFormat;
	
	/**
	 * SQL客户端配置
	 */
	public Properties config;
	
	/**
	 * 分页起始索引
	 */
	public Integer startIndex;
	
	/**
	 * 页面记录数(批处理尺寸)
	 */
	public Integer batchSize;
	
	/**
	 * MongoDB客户端
	 */
	public MongoClient mongoClient;
	
	/**
	 * MongoDB客户端连接参数
	 */
	private MongoClientOptions mongoClientOptions;
	
	/**
	 * MongoDB集合对象
	 */
	public MongoCollection<Document> mongoCollection;
	
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
	 * 过滤管道表
	 */
	public ArrayList<BasicDBObject> pipeLine=new ArrayList<BasicDBObject>();
	
	/**
	 * 主机地址表
	 */
	public ArrayList<ServerAddress> hostList=new ArrayList<ServerAddress>();
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MdbConfig.class);
	
	/**
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	public MdbConfig(){}
	
	public MdbConfig(Flow flow) {
		this.flow=flow;
		this.sourcePath=flow.sourcePath;
		this.config=PropertiesReader.getProperties(new File(sourcePath,"source.properties"));
	}
	
	/**
	 * @param config
	 */
	public MdbConfig config() {
		String dataBaseStr=config.getProperty("dataBaseName","").trim();
		if(dataBaseStr.isEmpty()) throw new RuntimeException("No Database Name Specified...");
		
		String collectionStr=config.getProperty("collectionName","").trim();
		if(collectionStr.isEmpty()) throw new RuntimeException("No Collection Name Specified...");
		
		initHostAddress();
		initSelectSqlParameter();
		initMongoClientOptions();
		
		String passWordStr=config.getProperty("passWord","").trim();
		String userNameStr=config.getProperty("userName","").trim();
		if(!passWordStr.isEmpty() && !userNameStr.isEmpty()) {
			userName=userNameStr;
			passWord=passWordStr;
		}
		
		String outFormatStr=config.getProperty("outFormat","").trim();
		this.outFormat=outFormatStr.isEmpty()?"qstr":outFormatStr;
		
		String realtimeStr=config.getProperty("realtime", "").trim();
		this.realtime=realtimeStr.isEmpty()?true:Boolean.parseBoolean(realtimeStr);
		
		if(null==userName || null==passWord) {
			mongoClient=new MongoClient(hostList,mongoClientOptions);
		}else{
			mongoClient=new MongoClient(hostList,MongoCredential.createCredential(userName, dataBaseStr, passWord.toCharArray()),mongoClientOptions);
		}
		
		MongoDatabase database=mongoClient.getDatabase(dataBaseStr);
		mongoCollection=database.getCollection(collectionStr,Document.class);
		
		return this;
	}
	
	/**
	 * 初始化查询SQL参数
	 */
	private void initSelectSqlParameter() {
		String selectSQLStr=config.getProperty("selectSQL","").trim();
		this.selectSQL=selectSQLStr.isEmpty()?"[{$skip:0},{$limit:100}]":selectSQLStr;
		
		if(!selectSQL.startsWith("[") || !selectSQL.endsWith("]")) {
			log.error("mongo sql must be array json format: [{..},{..}]");
			throw new RuntimeException("mongo sql must be array json format: [{..},{..}]");
		}
		
		if(!(selectSQL=selectSQL.substring(1, selectSQL.length()-1).trim()).isEmpty()) {
			String[] operArray=COMMA_REGEX.split(selectSQL);
			if(2>operArray.length) {
				log.error("mongo sql least have two parameter: {$skip:<startIndex>},{$limit:<batchSize>}");
				throw new RuntimeException("mongo sql least have two parameter: {$skip:<startIndex>},{$limit:<batchSize>}");
			}
			for(int i=0;i<operArray.length;pipeLine.add(BasicDBObject.parse(operArray[i++].trim())));
		}
		
		Number startIndexNum=(Number)pipeLine.get(pipeLine.size()-2).get("$skip");
		Number batchSizeNum=(Number)pipeLine.get(pipeLine.size()-1).get("$limit");
		if(null==startIndexNum || null==batchSizeNum) {
			log.error("last two pipe must be:{$skip:<startIndex>},{$limit:<batchSize>}");
			throw new RuntimeException("last two pipe must be:{$skip:<startIndex>},{$limit:<batchSize>}");
		}
		
		this.batchSize=batchSizeNum.intValue();
		this.startIndex=startIndexNum.intValue();
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress(){
		String hostListStr=config.getProperty("hostList", "").trim();
		String[] hosts=hostListStr.isEmpty()?new String[]{"127.0.0.1:27017"}:COMMA_REGEX.split(hostListStr);
		for(int i=0;i<hosts.length;i++) {
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
		
		String socketTimeout=config.getProperty("socketTimeout", "").trim();
		options.socketTimeout(socketTimeout.isEmpty()?0:Integer.parseInt(socketTimeout));
		
		String maxWaitTime=config.getProperty("maxWaitTime", "").trim();
		options.maxWaitTime(maxWaitTime.isEmpty()?5000:Integer.parseInt(maxWaitTime));
		
		String connectTimeout=config.getProperty("connectTimeout", "").trim();
		options.connectTimeout(connectTimeout.isEmpty()?30000:Integer.parseInt(connectTimeout));
		
		String connectionsPerHost=config.getProperty("connectionsPerHost", "").trim();
		options.connectionsPerHost(connectionsPerHost.isEmpty()?300:Integer.parseInt(connectionsPerHost));
		
		String cursorFinalizerEnabled=config.getProperty("cursorFinalizerEnabled", "").trim();
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
	 * 刷新数据表记录检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		OutputStream fos=null;
		config.setProperty("selectSQL",CommonUtil.javaToJsonStr(pipeLine));
		try{
			fos=new FileOutputStream(new File(sourcePath,"source.properties"));
			log.info("reflesh checkpoint...");
			config.store(fos, "reflesh checkpoint");
		}finally{
			if(null!=fos) fos.close();
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
		map.put("hostList", hostList);
		map.put("realtime", realtime);
		map.put("selectSQL", pipeLine);
		map.put("batchSize", batchSize);
		map.put("passWord", passWord);
		map.put("startIndex", startIndex);
		map.put("userName", userName);
		map.put("outFormat", outFormat);
		map.put("sourcePath", sourcePath);
		return map.toString();
	}
}
