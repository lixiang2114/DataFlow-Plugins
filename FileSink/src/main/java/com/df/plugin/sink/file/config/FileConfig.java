package com.df.plugin.sink.file.config;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.DateUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description 文件发送器配置
 */
@SuppressWarnings("unchecked")
public class FileConfig {
	/**
	 * 目标日志文件
	 */
	public File logFile;
	
	/**
	 * 发送器运行时路径
	 */
	private File sinkPath;
	
	/**
	 * 日志文件最大尺寸(默认100M)
	 */
	public Long maxFileSize;
	
	/**
	 * 文件客户端配置
	 */
	private Properties config;
	
	/**
	 * 日志文件创建时间
	 */
	public Calendar createTime;
	
	/**
	 * 日志文件流
	 */
	public OutputStream logFileStream;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileConfig.class);
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	public FileConfig(){}
	
	public FileConfig(Flow flow){
		this.sinkPath=flow.sinkPath;
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * 配置文件发送器
	 * @param config
	 */
	public FileConfig config() {
		String filePathStr=config.getProperty("filePath", "").trim();
		if(filePathStr.isEmpty()) {
			log.error("filePath parameter must be specify...");
			throw new RuntimeException("filePath value can not be empty...");
		}
		
		this.logFile=new File(filePathStr);
		if(logFile.exists() && logFile.isDirectory()) {
			log.error("filePath can not be directory...");
			throw new RuntimeException("filePath can not be directory...");
		}
		
		String maxHistoryStr=config.getProperty("maxHistory", "").trim();
		Integer maxHistory=maxHistoryStr.isEmpty()?1:Integer.parseInt(maxHistoryStr);
		
		this.maxFileSize=getMaxFileSize();
		try{
			File fileDir=logFile.getParentFile();
			if(!fileDir.exists()) fileDir.mkdirs();
			
			long expireInMills=maxHistory*86400*1000;
			long curTimeInMills=System.currentTimeMillis();
			for(File file:fileDir.listFiles()) if(curTimeInMills-Files.getFileAttributeView(Paths.get(file.toURI()), BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes().creationTime().toMillis()>=expireInMills) file.delete();
			
			logFileStream=Files.newOutputStream(logFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
			createTime=DateUtil.millSecondsToCalendar(logFile.lastModified());
		}catch(IOException e){
			throw new RuntimeException(e);
		}
		return this;
	}
	
	/**
	 * 获取日志文件最大尺寸
	 */
	private Long getMaxFileSize(){
		String configMaxVal=config.getProperty("maxFileSize", "").trim();
		if(configMaxVal.isEmpty()) return 100*1024*1024L;
		Matcher matcher=CAP_REGEX.matcher(configMaxVal);
		if(!matcher.find()) return 100*1024*1024L;
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
			Field field=FileConfig.class.getDeclaredField(attrName);
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
			Field field=FileConfig.class.getDeclaredField(attrName);
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
		map.put("logFile", logFile);
		map.put("maxFileSize", maxFileSize);
		return map.toString();
	}
}
