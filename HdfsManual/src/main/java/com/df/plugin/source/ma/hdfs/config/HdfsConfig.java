package com.df.plugin.source.ma.hdfs.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description HDFS分布式存储客户端配置
 */
@SuppressWarnings("unchecked")
public class HdfsConfig {
	/**
	 * 流程对象
	 */
	public Flow flow;
	
	/**
	 * 扫描的目标分布式文件
	 */
	public Path hdfsFile;
	
	/**
	 * 扫描的目标分布式目录
	 */
	public Path hdfsPath;
	
	/**
	 * 插件运行时路径
	 */
	public File pluginPath;
	
	/**
	 * 数据推送的缓冲尺寸
	 */
	public Integer bufferSize;
	
	/**
	 * Hdfs插件配置
	 */
	public Properties config;
	
	/**
	 * 离线文件已经读取的行数
	 */
	public long lineNumber;
	
	/**
	 * 离线文件已经读取的字节数量
	 */
	public long byteNumber;
	
	/**
	 * hadoop集群登录用户
	 */
	public String hadoopUser;
	
	/**
	 * 扫描类型
	 */
	public ScanType scanType;
	
	/**
	 * 文件系统
	 */
	public FileSystem fileSystem;
	
	/**
	 * 是否启用多线程处理文件
	 */
	public boolean multiThread;
	
	/**
	 * 已读取文件名检查点文件
	 */
	public java.nio.file.Path readedFile;
	
	/**
	 * 已读取的文件列表
	 */
	public HashSet<String> readedFileSet;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HdfsConfig.class);
	
	public HdfsConfig(){}
	
	public HdfsConfig(Flow flow){
		this.pluginPath=(this.flow=flow).sourcePath;
		this.config=PropertiesReader.getProperties(new File(pluginPath,"source.properties"));
	}
	
	/**
	 * @param config
	 */
	public HdfsConfig config() throws IOException{
		String scanTypeStr=config.getProperty("scanType","").trim();
		this.scanType=scanTypeStr.isEmpty()?ScanType.file:ScanType.valueOf(scanTypeStr);
		
		String hadoopUserStr=config.getProperty("hadoopUser", "").trim();
		this.hadoopUser=hadoopUserStr.isEmpty()?"hadoop":hadoopUserStr;
		
		if(ScanType.file==scanType) {
			String hdfsFileName=config.getProperty("hdfsFile","").trim();
			if(hdfsFileName.isEmpty()) {
				log.error("not found parameter: 'hdfsFile',can not offline scan with manual...");
				throw new RuntimeException("not found parameter: 'hdfsFile',can not offline scan with manual...");
			}else{
				Path hdfsFile=new Path(hdfsFileName);
				try {
					this.fileSystem=FileSystem.get(hdfsFile.toUri(), new Configuration(), hadoopUser);
				} catch (Exception e) {
					log.error("create file system occur exception...");
					throw new RuntimeException("create file system occur exception...",e);
				}
				
				if(!fileSystem.exists(hdfsFile)){
					log.error("specify hdfsFile is not exists: {}",hdfsFileName);
					throw new RuntimeException("specify hdfsFile is not exists: "+hdfsFileName);
				}
				
				if(fileSystem.getFileStatus(hdfsFile).isDirectory()){
					log.error("specify hdfsFile is an directory: {}",hdfsFileName);
					throw new RuntimeException("specify hdfsFile is an directory: "+hdfsFileName);
				}
				
				this.hdfsFile=hdfsFile;
			}
		}else{
			String hdfsPathName=config.getProperty("hdfsPath","").trim();
			if(hdfsPathName.isEmpty()) {
				log.error("not found parameter: 'hdfsPath',can not offline scan with manual...");
				throw new RuntimeException("not found parameter: 'hdfsPath',can not offline scan with manual...");
			}else{
				Path hdfsPath=new Path(hdfsPathName);
				try {
					this.fileSystem=FileSystem.get(hdfsPath.toUri(), new Configuration(), hadoopUser);
				} catch (Exception e) {
					log.error("create file system occur exception...");
					throw new RuntimeException("create file system occur exception...",e);
				}
				
				if(!fileSystem.exists(hdfsPath)) {
					log.error("specify path is not exists: {}",hdfsPathName);
					throw new RuntimeException("specify path is not exists: "+hdfsPathName);
				}
				
				if(fileSystem.getFileStatus(hdfsPath).isFile()){
					log.error("specify path is an file: {}",hdfsPathName);
					throw new RuntimeException("specify path is an file: "+hdfsPathName);
				}
				
				this.hdfsPath=hdfsPath;
			}
		}
		
		String multiThreadStr=config.getProperty("multiThread","").trim();
		this.multiThread=multiThreadStr.isEmpty()?false:Boolean.parseBoolean(multiThreadStr);
		
		if(!multiThread && ScanType.path==scanType) {
			String readedFileStr=config.getProperty("readedFile","").trim();
			this.readedFile=new File(pluginPath,readedFileStr.isEmpty()?"readedFiles.ini":readedFileStr).toPath();
			try {
				this.readedFileSet=new HashSet<String>(Files.readAllLines(readedFile, StandardCharsets.UTF_8));
			} catch (IOException e) {
				log.error("read file readed occur error...",e);
			}
		}
		
		String bufferSizeStr=config.getProperty("bufferSize", "").trim();
		this.bufferSize=bufferSizeStr.isEmpty()?4096:Integer.parseInt(bufferSizeStr);
		
		String lineNumberStr=config.getProperty("lineNumber","").trim();
		this.lineNumber=lineNumberStr.isEmpty()?0:Long.parseLong(lineNumberStr);
		
		String byteNumberStr=config.getProperty("byteNumber","").trim();
		this.byteNumber=byteNumberStr.isEmpty()?0:Long.parseLong(byteNumberStr);
		
		log.info("lineNumber is: "+lineNumber+",byteNumber is: "+byteNumber);
		return this;
	}
	
	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		config.setProperty("lineNumber",""+lineNumber);
		config.setProperty("byteNumber",""+byteNumber);
		if(null!=hdfsFile) config.setProperty("hdfsFile", hdfsFile.toString());
		if(null!=hdfsPath) config.setProperty("hdfsPath", hdfsPath.toString());
		
		if(null!=readedFileSet && !readedFileSet.isEmpty()) {
			if(null!=readedFile) Files.write(readedFile, readedFileSet, StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING);
		}
		
		OutputStream fos=null;
		try{
			fos=new FileOutputStream(new File(pluginPath,"source.properties"));
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
		map.put("hdfsPath", hdfsPath);
		map.put("scanType", scanType);
		map.put("readedFile", readedFile);
		map.put("pluginPath", pluginPath);
		map.put("lineNumber", lineNumber);
		map.put("multiThread", multiThread);
		map.put("hadoopUser", hadoopUser);
		map.put("byteNumber", byteNumber);
		map.put("readedFileSet", readedFileSet);
		return map.toString();
	}
}
