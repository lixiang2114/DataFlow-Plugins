package com.df.plugin.sink.hbase.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.hbase.dto.FieldMapper;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Hbase发送器配置
 */
@SuppressWarnings("unchecked")
public class HbaseConfig {
	/**
	 * 批处理尺寸
	 */
	public int batchSize;
	
	/**
	 * 发送器运行时路径
	 */
	private File sinkPath;
	
	/**
	 * 字段值最大版本号
	 */
	public int maxVersion;
	
	/**
	 * 是否需要解析通道记录
	 */
	public boolean parse;
	
	/**
	 * 默认数据库表名
	 * 默认命名空间为:default
	 */
	public String defaultTab;
	
	/**
	 * Hbase客户端配置
	 */
	private Properties config;
	
	/**
	 * 是否自动创建库表空间
	 */
	public boolean autoCreate;
	
	/**
	 * 字段分隔符
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
	 * 通道字段映射器
	 */
	public FieldMapper fieldMapper;
	
	/**
	 * 写入Hbase数据库的时间字段集
	 * 通常为java.util.Date的子类型
	 */
	public Set<String> timeFieldSet;
	
	/**
	 * 写入Hbase数据库的数字字段集
	 * 通常为java.lang.Number的子类型
	 */
	public Set<String> numFieldSet;
	
	/**
	 * 上次发送失败任务表
	 */
	public Set<Object> preFailSinkSet;
	
	/**
	 * Zookeper主机列表
	 */
	public ArrayList<String> hostList=new ArrayList<String>();
	
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
	private static final Logger log=LoggerFactory.getLogger(HbaseConfig.class);
	
	/**
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	public HbaseConfig(){}
	
	public HbaseConfig(Flow flow){
		this.sinkPath=flow.sinkPath;
		this.preFailSinkSet=flow.preFailSinkSet;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * 配置分布式存储发送器
	 * @param config
	 */
	public HbaseConfig config() throws IOException{
		initHostAddress();
		if(hostList.isEmpty()) {
			log.error("hostList parameter must be specify...");
			throw new RuntimeException("hostList parameter must be specify...");
		}
		
		String parseStr=config.getProperty("parse", "").trim();
		this.parse=parseStr.isEmpty()?false:Boolean.parseBoolean(parseStr);
		
		String batchSizeStr=config.getProperty("batchSize", "").trim();
		this.batchSize=batchSizeStr.isEmpty()?100:Integer.parseInt(batchSizeStr);
		
		String maxVersionStr=config.getProperty("maxVersion", "").trim();
		this.maxVersion=maxVersionStr.isEmpty()?3:Integer.parseInt(maxVersionStr);
		
		String autoCreateStr=config.getProperty("autoCreate", "").trim();
		this.autoCreate=autoCreateStr.isEmpty()?true:Boolean.parseBoolean(autoCreateStr);
		
		String fieldSeparatorStr=config.getProperty("fieldSeparator", "").trim();
		this.fieldSeparator=Pattern.compile(fieldSeparatorStr.isEmpty()?"#":fieldSeparatorStr);
		
		String maxRetryTimeStr=config.getProperty("maxRetryTimes","").trim();
		this.maxRetryTimes=maxRetryTimeStr.isEmpty()?3:Integer.parseInt(maxRetryTimeStr);
		
		String failMaxTimeMillStr=config.getProperty("failMaxTimeMills","").trim();
		this.failMaxWaitMills=failMaxTimeMillStr.isEmpty()?2000:Long.parseLong(failMaxTimeMillStr);
		
		String batchMaxTimeMillStr=config.getProperty("batchMaxTimeMills","").trim();
		this.batchMaxWaitMills=batchMaxTimeMillStr.isEmpty()?15000:Long.parseLong(batchMaxTimeMillStr);
		
		String defaultTabStr=config.getProperty("defaultTab", "").trim();
		this.defaultTab=(defaultTabStr.isEmpty() || !defaultTabStr.contains(":"))?"default:defaultTab":defaultTabStr;
		
		String numFieldStr=config.getProperty("numFields","").trim();
		this.numFieldSet=numFieldStr.isEmpty()?new HashSet<String>():Arrays.stream(COMMA_REGEX.split(numFieldStr.trim()))
				.map(e->e.trim())
				.filter(e->!e.isEmpty())
				.collect(Collectors.toSet());
		
		String timeFieldStr=config.getProperty("timeFields","").trim();
		this.timeFieldSet=timeFieldStr.isEmpty()?new HashSet<String>():Arrays.stream(COMMA_REGEX.split(timeFieldStr.trim()))
				.map(e->e.trim())
				.filter(e->!e.isEmpty())
				.collect(Collectors.toSet());
		
		if(numFieldSet.retainAll(timeFieldSet)) {
			log.error("numFieldSet and timeFieldSet  can not be intersection...");
			throw new RuntimeException("numFieldSet and timeFieldSet  can not be intersection...");
		}
		
		String fieldMapStr=config.getProperty("fieldMap", "").trim();
		if(fieldMapStr.isEmpty()) {
			log.error("fieldMap can not be empty...");
			throw new RuntimeException("fieldMap can not be empty...");
		}
		this.fieldMapper=new FieldMapper(CommonUtil.jsonStrToJava(fieldMapStr, Map.class),"tabField","rowField","familyField","keyField","valueField");
		
		if(parse) {
			String parseFieldStr=config.getProperty("parseFields", "").trim();
			if(parseFieldStr.isEmpty()) {
				log.error("parseList can not be empty when parse is true...");
				throw new RuntimeException("parseList can not be empty when parse is true...");
			}
			
			String[] parseArray=COMMA_REGEX.split(parseFieldStr);
			if(5>parseArray.length) {
				log.error("parseList must have 5 fields: table,rowKey,family,key,value");
				throw new RuntimeException("parseList must have 5 fields: table,rowKey,family,key,value");
			}
			
			String[] parseFields=new String[5];
			for(int i=0;i<parseFields.length;i++) {
				parseFields[i]=parseArray[i].trim();
				if(parseFields[i].isEmpty()) {
					log.error("parseField can not be empty...");
					throw new RuntimeException("parseField can not be empty...");
				}
			}
			
			fieldMapper.setParseFields(parseFields);
		}
		
		return this;
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress() {
		String hostListStr=config.getProperty("hostList","").trim();
		String[] hosts=hostListStr.isEmpty()?new String[]{"127.0.0.1:2181"}:COMMA_REGEX.split(hostListStr);
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
				hostList.add(unknow+":2181");
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
			Field field=HbaseConfig.class.getDeclaredField(attrName);
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
			Field field=HbaseConfig.class.getDeclaredField(attrName);
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
		map.put("batchSize", batchSize);
		map.put("defaultTab", defaultTab);
		map.put("autoCreate", autoCreate);
		map.put("maxVersion", maxVersion);
		map.put("fieldMapper", fieldMapper);
		map.put("timeFieldSet", timeFieldSet);
		map.put("numFieldSet", numFieldSet);
		map.put("maxRetryTimes", maxRetryTimes);
		map.put("failMaxWaitMills", failMaxWaitMills);
		map.put("fieldSeparator", fieldSeparator.pattern());
		map.put("batchMaxWaitMills", batchMaxWaitMills);
		return map.toString();
	}
}
