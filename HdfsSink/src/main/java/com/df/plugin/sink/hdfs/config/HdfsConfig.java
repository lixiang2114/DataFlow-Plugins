package com.df.plugin.sink.hdfs.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Hdfs发送器配置
 */
@SuppressWarnings("unchecked")
public class HdfsConfig {
	/**
	 * 目标数据文件
	 */
	public Path hdfsFile;
	
	/**
	 * 发送器运行时路径
	 */
	private File sinkPath;
	
	/**
	 * 批处理缓冲文件
	 */
	public File batchFile;
	
	/**
	 * 数据文件最大尺寸(默认10GB)
	 */
	public Long maxFileSize;
	
	/**
	 * 历史数据文件钝化最大时间(单位:天)
	 * 默认为30天,超过钝化时间没有访问则会被自动删除
	 */
	public Integer maxHistory;
	
	/**
	 * 数据推送的缓冲尺寸
	 */
	public Integer bufferSize;
	
	/**
	 * 文件客户端配置
	 */
	private Properties config;
	
	/**
	 * hadoop集群登录用户
	 */
	public String hadoopUser;
	
	/**
	 * 文件系统
	 */
	public FileSystem fileSystem;
	
	/**
	 * 离线推送的最大批处理字节数
	 */
	public Integer maxBatchBytes;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HdfsConfig.class);
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	public HdfsConfig(){}
	
	public HdfsConfig(Flow flow){
		this.sinkPath=flow.sinkPath;
		this.batchFile=new File(sinkPath,"tmp/buffer.dat");
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * 配置分布式存储发送器
	 * @param config
	 */
	public HdfsConfig config() throws IOException{
		String hdfsFileStr=config.getProperty("hdfsFile", "").trim();
		if(hdfsFileStr.isEmpty()) {
			log.error("hdfsFile parameter must be specify...");
			throw new RuntimeException("hdfsFile value can not be empty...");
		}
		
		this.hdfsFile=new Path(hdfsFileStr+".0");
		String hadoopUserStr=config.getProperty("hadoopUser", "").trim();
		this.hadoopUser=hadoopUserStr.isEmpty()?"hadoop":hadoopUserStr;
		
		try {
			this.fileSystem=FileSystem.get(hdfsFile.toUri(), new Configuration(), hadoopUser);
		} catch (Exception e) {
			log.error("create file system occur exception...");
			throw new RuntimeException("create file system occur exception...");
		}
		
		if(fileSystem.exists(hdfsFile) && fileSystem.getFileStatus(hdfsFile).isDirectory()) {
			log.error("hdfsFile can not be directory...");
			throw new RuntimeException("hdfsFile can not be directory...");
		}
		
		Path hdfsPath=hdfsFile.getParent();
		if(!fileSystem.exists(hdfsPath)) fileSystem.mkdirs(hdfsPath);
		
		this.maxFileSize=getMaxFileSize();
		
		String bufferSizeStr=config.getProperty("bufferSize", "").trim();
		this.bufferSize=bufferSizeStr.isEmpty()?4096:Integer.parseInt(bufferSizeStr);
		
		String maxHistoryStr=config.getProperty("maxHistory", "").trim();
		this.maxHistory=maxHistoryStr.isEmpty()?30:Integer.parseInt(maxHistoryStr);
		
		String maxBatchByteStr=config.getProperty("maxBatchBytes", "").trim();
		this.maxBatchBytes=maxBatchByteStr.isEmpty()?100:Integer.parseInt(maxBatchByteStr);
		
		RemoteIterator<LocatedFileStatus> fileList=fileSystem.listFiles(hdfsPath, true);
		while(fileList.hasNext()) {
			LocatedFileStatus fileStatus=fileList.next();
			long expireDays=(System.currentTimeMillis()-fileStatus.getAccessTime())/86400000;
			if(expireDays>maxHistory) {
				Path expireFile=fileStatus.getPath();
				fileSystem.delete(expireFile, true);
				log.info("expire timeout: {}d,delete expire hdfs file: {}",expireDays,expireFile.toString());
			}
		}
		
		return this;
	}
	
	/**
	 * 获取数据文件最大尺寸
	 * 默认10GB
	 */
	private Long getMaxFileSize(){
		String configMaxVal=config.getProperty("maxFileSize", "").trim();
		if(configMaxVal.isEmpty()) return 10*1024*1024L*1024;
		Matcher matcher=CAP_REGEX.matcher(configMaxVal);
		if(!matcher.find()) return 10*1024*1024L*1024;
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
			Field field=HdfsConfig.class.getDeclaredField(attrName);
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
			Field field=HdfsConfig.class.getDeclaredField(attrName);
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
		map.put("hdfsFile", hdfsFile);
		map.put("bufferSize", bufferSize);
		map.put("maxHistory", maxHistory);
		map.put("maxFileSize", maxFileSize);
		map.put("hadoopUser", hadoopUser);
		map.put("maxBatchBytes", maxBatchBytes);
		return map.toString();
	}
}
