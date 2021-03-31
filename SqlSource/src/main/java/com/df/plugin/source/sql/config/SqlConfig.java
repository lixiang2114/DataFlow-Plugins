package com.df.plugin.source.sql.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description SQL客户端配置
 * 常见数据库分页SQL关键字:
 * endIndex=startIndex+batchSize
 * MySql语法: limit startIndex,batchSize
 * PostgreSQL语法: limit batchSize,offset startIndex
 * SqlServer语法: offset startIndex rows fetch next batchSize rows
 * Oracle语法: where rownum>startIndex and rownum<endIndex
 * DB2语法: where rownum>startIndex and rownum<endIndex
 */
@SuppressWarnings("unchecked")
public class SqlConfig {
	/**
	 * 当前流程实例对象
	 */
	public Flow flow;
	
	/**
	 * 收集器运行时路径
	 */
	public File sourcePath;
	
	/**
	 * SQL服务器用户
	 */
	public String userName;
	
	/**
	 * 是否为实时读取
	 */
	public Boolean realtime;
	
	/**
	 * SQL服务器密码
	 */
	public String passWord;
	
	/**
	 * 拉取数据SQL语句
	 */
	public String selectSQL;
	
	/**
	 * 占位符字段名(必须与SQL中的占位符对应)
	 * startIndex:起始记录索引
	 * batchSize:页面记录数量
	 * endIndex:结束记录索引
	 */
	public String[] phFields;
	
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
	 * JDBC驱动连接器
	 */
	private String jdbcDriver;
	
	/**
	 * SQL服务器连接字符串
	 */
	public String connectionString;
	
	/**
	 * SQL客户端连接
	 */
	private Connection connection;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(SqlConfig.class);
	
	/**
	 * 分页字典,它包含以下键(与phFields对应)
	 * batchSize:每页记录数量(批处理尺寸)
	 * startIndex:起始记录索引(结果集包含该索引)
	 * endIndex:结束记录索引(结果集不包含该索引)
	 */
	public ConcurrentHashMap<String,Integer> pageDict=new ConcurrentHashMap<String,Integer>();
	
	/**
	 * 分页占位符字段名常量
	 */
	private static final HashSet<String> PH_FIELDS=new HashSet<String>(Arrays.asList("startIndex","batchSize","endIndex"));
	
	public SqlConfig(){}
	
	public SqlConfig(Flow flow) {
		this.flow=flow;
		this.sourcePath=flow.sourcePath;
		this.config=PropertiesReader.getProperties(new File(sourcePath,"source.properties"));
	}
	
	/**
	 * @param config
	 */
	public SqlConfig config() {
		this.selectSQL=config.getProperty("selectSQL","").trim();
		if(selectSQL.isEmpty()) {
			log.error("No Select SQL Specified,Parameter Name: selectSQL");
			throw new RuntimeException("No Select SQL Specified,Parameter Name: selectSQL");
		}
		
		String jdbcDriverStr=config.getProperty("jdbcDriver","").trim();
		this.jdbcDriver=jdbcDriverStr.isEmpty()?"com.mysql.cj.jdbc.Driver":jdbcDriverStr;
		
		String connectionString=config.getProperty("connectionString","").trim();
		this.connectionString=connectionString.isEmpty()?"jdbc:mysql://192.168.162.127:3306/test?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&useSSL=false&serverTimezone=GMT%2B8":connectionString;
		
		String phFieldsStr=config.getProperty("phFields","").trim();
		if(phFieldsStr.isEmpty()) {
			log.error("No Placeholder Fields Specified,Parameter Name: phFields");
			throw new RuntimeException("No Placeholder Fields Specified,Parameter Name: phFields");
		}else{
			String[] pageFields=COMMA_REGEX.split(phFieldsStr);
			if(2>pageFields.length) {
				log.error("Not Enough Parameters,At Least Two Parameters Are Required!");
				throw new RuntimeException("Not Enough Parameters,At Least Two Parameters Are Required!");
			}
			
			String tmpField=null;
			for(int i=0;i<pageFields.length;i++){
				tmpField=pageFields[i].trim();
				if(!PH_FIELDS.contains(tmpField)) {
					log.error("Wrong Placeholder Field Name,Support Field Name:"+PH_FIELDS);
					throw new RuntimeException("Wrong Placeholder Field Name,Support Field Name:"+PH_FIELDS);
				}
				pageFields[i]=tmpField;
			}
			this.phFields=pageFields;
		}
		
		String passWordStr=config.getProperty("passWord","").trim();
		String userNameStr=config.getProperty("userName","").trim();
		if(!passWordStr.isEmpty() && !userNameStr.isEmpty()) {
			this.userName=userNameStr;
			this.passWord=passWordStr;
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
		
		String outFormatStr=config.getProperty("outFormat", "").trim();
		this.outFormat=outFormatStr.isEmpty()?"qstr":outFormatStr;
		
		String realtimeStr=config.getProperty("realtime", "").trim();
		this.realtime=realtimeStr.isEmpty()?true:Boolean.parseBoolean(realtimeStr);
		
		String batchSizeStr=config.getProperty("batchSize", "").trim();
		Integer batchSize=batchSizeStr.isEmpty()?100:Integer.parseInt(batchSizeStr);
		
		String startIndexStr=config.getProperty("startIndex", "").trim();
		Integer startIndex=startIndexStr.isEmpty()?0:Integer.parseInt(startIndexStr);
		
		pageDict.put("endIndex", startIndex+batchSize);
		pageDict.put("startIndex", startIndex);
		pageDict.put("batchSize", batchSize);
		
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
	 * 刷新数据表记录检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		OutputStream fos=null;
		config.setProperty("startIndex",pageDict.get("startIndex").toString());
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
		map.put("realtime", realtime);
		map.put("pageDict", pageDict);
		map.put("selectSQL", selectSQL);
		map.put("passWord", passWord);
		map.put("userName", userName);
		map.put("jdbcDriver", jdbcDriver);
		map.put("outFormat", outFormat);
		map.put("sourcePath", sourcePath);
		map.put("pageFieldNames", PH_FIELDS);
		map.put("phFields", Arrays.toString(phFields));
		map.put("connectionString", connectionString);
		return map.toString();
	}
}
