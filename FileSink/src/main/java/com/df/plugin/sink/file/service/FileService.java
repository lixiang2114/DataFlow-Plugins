package com.df.plugin.sink.file.service;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Calendar;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.file.config.FileConfig;
import com.github.lixiang2114.flow.util.DateUtil;

/**
 * @author Lixiang
 * @description 文件服务
 */
public class FileService {
	/**
	 * 文件发送器配置
	 */
	private FileConfig fileConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileService.class);
	
	/**
	 * 日志文件命名计数器
	 */
	private HashMap<String,Integer> nameCounter=new HashMap<String,Integer>();
	
	public FileService(){}
	
	public FileService(FileConfig fileConfig){
		this.fileConfig=fileConfig;
	}
	
	/**
	 * 写出日志文件
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean writeMessage(String message) throws Exception {
		boolean isToday=false;
		try {
			if(!(isToday=DateUtil.isToday(fileConfig.createTime)) || fileConfig.maxFileSize<=fileConfig.logFile.length()) {
				fileConfig.logFileStream.flush();
				fileConfig.logFileStream.close();
				renameFile(isToday,fileConfig.createTime);
				fileConfig.logFileStream=Files.newOutputStream(fileConfig.logFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
				fileConfig.createTime=DateUtil.millSecondsToCalendar(fileConfig.logFile.lastModified());
			}
			fileConfig.logFileStream.write((message+"\n").getBytes(StandardCharsets.UTF_8));
			fileConfig.logFileStream.flush();
			return true;
		} catch (IOException e) {
			log.error("write message occur error: ",e);
		}
		return false;
	}
	
	/**
	 * 检查日志文件滚动命名
	 * @param isToday 是否是今天
	 * @throws creationTime 文件创建时间
	 * @throws IOException 
	 */
	private void renameFile(Boolean isToday,Calendar creationTime) throws IOException {
		String createTimeStr=new StringBuilder("").append(creationTime.get(Calendar.YEAR)).append("-").append(creationTime.get(Calendar.MONTH)+1).append("-").append(creationTime.get(Calendar.DATE)).toString();
    	Integer fileCounter=nameCounter.get(createTimeStr);
    	if(null==fileCounter) nameCounter.put(createTimeStr, fileCounter=0);
    	
    	String newName=new StringBuilder(fileConfig.logFile.getAbsolutePath()).append("-").append(createTimeStr).append(".").append(fileCounter).toString();
    	fileConfig.logFile.renameTo(new File(newName));
    	
    	if(isToday) {
    		nameCounter.put(createTimeStr, fileCounter+1);
    	}else{
    		nameCounter.clear();
    	}
	}
	
	/**
	 * 停止文件流进程
	 * @throws IOException
	 */
	public void stop() throws IOException {
		if(null!=fileConfig.logFileStream) fileConfig.logFileStream.close();
		nameCounter.clear();
		nameCounter=null;
	}
}
