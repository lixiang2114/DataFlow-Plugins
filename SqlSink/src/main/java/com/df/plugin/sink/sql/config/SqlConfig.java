package com.df.plugin.sink.sql.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.sql.dto.SQLMapper;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description SQL客户端配置
 */
@SuppressWarnings("unchecked")
public class SqlConfig {
	/**
	 * 发送器运行时路径
	 */
	public File sinkPath;
	
	/**
	 * 是否解析通道数据记录
	 */
	public boolean parse;
	
	/**
	 * 处理记录中的库名
	 */
	public String dbField;
	
	/**
	 * 处理记录中的表名
	 */
	public String tabField;
	
	/**
	 * 处理记录中库名索引
	 */
	public Integer dbIndex;
	
	/**
	 * 处理记录中表名索引
	 */
	public Integer tabIndex;
	
	/**
	 * SQL服务器用户
	 */
	public String userName;
	
	/**
	 * SQL服务器密码
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
	 * 批处理尺寸
	 */
	public Integer batchSize;
	
	/**
	 * SQL客户端配置
	 */
	public Properties config;
	
	/**
	 * JDBC驱动连接器
	 */
	private String jdbcDriver;
	
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
	 * 记录字段默认分隔符为中英文空白正则式
	 */
	public Pattern fieldSeparator;
	
	/**
	 * SQL客户端连接
	 */
	private Connection connection;
	
	/**
	 * SQL服务器连接字符串
	 */
	public String connectionString;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * SQL缓存字典
	 */
	private ConcurrentHashMap<String,SQLMapper> sqlCache;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(SqlConfig.class);
	
	public SqlConfig(Flow flow) {
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		sqlCache=new ConcurrentHashMap<String,SQLMapper>();
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * @param config
	 */
	public SqlConfig config() {
		String jdbcDriverStr=config.getProperty("jdbcDriver","").trim();
		this.jdbcDriver=jdbcDriverStr.isEmpty()?"com.mysql.cj.jdbc.Driver":jdbcDriverStr;
		
		String connectionString=config.getProperty("connectionString","").trim();
		this.connectionString=connectionString.isEmpty()?"jdbc:mysql://127.0.0.1:3306/?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&useSSL=false&serverTimezone=GMT%2B8":connectionString;
		
		String tabIndexStr=config.getProperty("tabIndex","").trim();
		this.tabIndex=tabIndexStr.isEmpty()?null:Integer.parseInt(tabIndexStr);
		
		String dbIndexStr=config.getProperty("dbIndex","").trim();
		this.dbIndex=dbIndexStr.isEmpty()?null:Integer.parseInt(dbIndexStr);
		
		if(null!=tabIndex && null!=dbIndex && tabIndex.intValue()==dbIndex.intValue()) {
			log.error("dbIndex can not equal tabIndex: {}",tabIndex);
			throw new RuntimeException("dbIndex can not equal tabIndex: "+tabIndex);
		}
		
		String defaultTabStr=config.getProperty("defaultTab","").trim();
		this.defaultTab=defaultTabStr.isEmpty()?null:defaultTabStr;
		
		String defaultDBStr=config.getProperty("defaultDB","").trim();
		this.defaultDB=defaultDBStr.isEmpty()?null:defaultDBStr;
		
		String tabFieldStr=config.getProperty("tabField","").trim();
		this.tabField=tabFieldStr.isEmpty()?null:tabFieldStr;
		
		String dbFieldStr=config.getProperty("dbField","").trim();
		this.dbField=dbFieldStr.isEmpty()?null:dbFieldStr;
		
		String passWordStr=config.getProperty("passWord","").trim();
		String userNameStr=config.getProperty("userName","").trim();
		if(!passWordStr.isEmpty() && !userNameStr.isEmpty()) {
			userName=userNameStr;
			passWord=passWordStr;
		}
		
		try {
			Class.forName(jdbcDriver);
		} catch (ClassNotFoundException e) {
			log.error("Can Not Load And Init Jdbc Driver:",e);
			throw new RuntimeException(e);
		}
		
		try{
			this.getConnection();
		}catch(SQLException e){
			log.error("create connection occur exeption:",e);
			throw new RuntimeException(e);
		}
		
		String batchSizeStr=config.getProperty("batchSize","").trim();
		if(!batchSizeStr.isEmpty()) this.batchSize=Integer.parseInt(batchSizeStr);
		
		String parseStr=config.getProperty("parse","").trim();
		this.parse=parseStr.isEmpty()?true:Boolean.parseBoolean(parseStr);
		
		String fieldSeparatorStr=config.getProperty("fieldSeparator","").trim();
		this.fieldSeparator=Pattern.compile(fieldSeparatorStr.isEmpty()?"\\s+":fieldSeparatorStr);
		
		String maxRetryTimeStr=config.getProperty("maxRetryTimes","").trim();
		this.maxRetryTimes=maxRetryTimeStr.isEmpty()?3:Integer.parseInt(maxRetryTimeStr);
		
		String failMaxTimeMillStr=config.getProperty("failMaxTimeMills","").trim();
		this.failMaxWaitMills=failMaxTimeMillStr.isEmpty()?2000:Long.parseLong(failMaxTimeMillStr);
		
		String batchMaxTimeMillStr=config.getProperty("batchMaxTimeMills","").trim();
		this.batchMaxWaitMills=batchMaxTimeMillStr.isEmpty()?2000:Long.parseLong(batchMaxTimeMillStr);
		
		return this;
	}
	
	/**
	 * 初始化SQL配置
	 */
	public Connection getConnection() throws SQLException {
		if(null!=connection && !connection.isClosed()) return connection;
		if(null==userName || null==passWord) {
			return connection=DriverManager.getConnection(connectionString);
		}else{
			return connection=DriverManager.getConnection(connectionString, userName, passWord);
		}
	}
	
	/**
	 * 获取SQL映射器
	 * @param tabFullName 数据库表全名
	 * @return SQL映射器
	 * @throws Exception
	 */
	public SQLMapper getSqlMapper(String tabFullName) throws Exception {
		if(isEmpty(tabFullName)) return null;
		
		SQLMapper sqlMapper=sqlCache.get(tabFullName);
		if(null!=sqlMapper) return sqlMapper;
		
		ResultSet res=null;
		Statement stat=null;
		LinkedHashMap<String,Class<?>> fieldMap=new LinkedHashMap<String,Class<?>>();
		StringBuilder sqlBuilder=new StringBuilder("insert into ").append(tabFullName).append(" (");
		try {
			stat=getConnection().createStatement();
			res=stat.executeQuery("select * from "+tabFullName+" where 1=2");
			ResultSetMetaData rsmd=res.getMetaData();
			int len=rsmd.getColumnCount();
			if(1>len) {
				log.error("Not Found Any Field From table: {}",tabFullName);
				throw new RuntimeException("Not Found Any Field From table: "+tabFullName);
			}
			
			String fieldName=null;
			for(int i=1;i<=len;i++){
				fieldName=rsmd.getColumnLabel(i);
				sqlBuilder.append(fieldName).append(",");
				fieldMap.put(fieldName, Class.forName(rsmd.getColumnClassName(i)));
			}
			
			sqlBuilder.replace(sqlBuilder.length()-1, sqlBuilder.length(), ") values (");
			for(int i=1;i<=len;sqlBuilder.append("?,"),i++);
			sqlBuilder.replace(sqlBuilder.length()-1, sqlBuilder.length(), ")");
			String insertSQL=sqlBuilder.toString();
			
			sqlCache.put(tabFullName, sqlMapper=new SQLMapper(tabFullName,insertSQL,fieldMap,getConnection()));
			log.info("add new fieldMap: {}",fieldMap);
			log.info("add new insertSQL: {}",insertSQL);
			return sqlMapper;
		} finally {
			if(null!=res) res.close();
			if(null!=stat) stat.close();
		}
	}
	
	/**
	 * 获取参数值
	 * @param key 参数名
	 * @param defaultValue 默认参数值
	 * @return 参数值
	 */
	private static final boolean isEmpty(String value) {
		if(null==value) return true;
		return value.trim().isEmpty();
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
			Field field=SqlConfig.class.getDeclaredField(attrName);
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
			Field field=SqlConfig.class.getDeclaredField(attrName);
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
		map.put("table", defaultDB);
		map.put("table", defaultTab);
		map.put("dbIndex", dbIndex);
		map.put("sinkPath", sinkPath);
		map.put("tabIndex", tabIndex);
		map.put("sqlCache", sqlCache);
		map.put("batchSize", batchSize);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("jdbcDriver", jdbcDriver);
		map.put("fieldSeparator", fieldSeparator);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("connectionString", connectionString);
		map.put("batchMaxWaitMills", batchMaxWaitMills);
		map.put("preFailSinkSetSize", preFailSinkSet.size());
		return map.toString();
	}
}
