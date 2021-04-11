package com.df.plugin.transfer.redis.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.redis.config.RedisConfig;
import com.df.plugin.transfer.redis.util.RedisUtil;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description Redis服务模块
 */
public class RedisService {
	/**
	 * Redis配置
	 */
	private RedisConfig redisConfig;
	
	/**
	 * Redis客户端工具
	 */
	private RedisUtil redisUtil;
	
	/**
	 * 转存缓冲输出器
	 */
	private BufferedWriter bufferedWriter;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(RedisService.class);
	
	public RedisService() {}
	
	public RedisService(RedisConfig redisConfig) {
		this.redisConfig=redisConfig;
		this.redisUtil=redisConfig.redisUtil;
		try {
			bufferedWriter=Files.newBufferedWriter(redisConfig.transferSaveFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		} catch (IOException e) {
			log.error("transferSaveFile is not exists: ",e);
		}
	}
	
	/**
	 * 启动扫描器
	 * @throws Exception
	 */
	public void startScanner() throws Exception {
		while(redisConfig.flow.transferStart) {
			for(String targetPipe:redisConfig.targetPipes) {
				if(targetPipe.isEmpty()) continue;
				
				Object object=redisUtil.rightPop(targetPipe, redisConfig.pollTimeoutMills);
				if(null==object) continue;
				
				String line=CommonUtil.toString(object);
				if(null==line || line.isEmpty()) continue;
	        	
	        	//写入转存日志文件
	        	bufferedWriter.write(line);
	        	bufferedWriter.newLine();
	        	bufferedWriter.flush();
	        	
	        	//当前转存日志文件未达到最大值则继续写转存日志文件
	        	if(redisConfig.transferSaveFile.length()<redisConfig.transferSaveMaxSize) continue;
	        	
	        	//当前转存日志文件达到最大值则增加转存日志文件
	        	String curTransSaveFilePath=redisConfig.transferSaveFile.getAbsolutePath();
	        	int lastIndex=curTransSaveFilePath.lastIndexOf(".");
	        	if(-1==lastIndex) {
					lastIndex=curTransSaveFilePath.length();
					curTransSaveFilePath=curTransSaveFilePath+".0";
				}
	        	redisConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
	        	log.info("RedisTransfer switch transfer save log file to: "+redisConfig.transferSaveFile.getAbsolutePath());
	        	
	        	try{
	        		bufferedWriter.close();
	        	}catch(IOException e){
	        		log.error("close transferSaveFile stream occur error: ",e);
	        	}
	        	
	        	bufferedWriter=Files.newBufferedWriter(redisConfig.transferSaveFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,StandardOpenOption.APPEND);
			}
	    }
	}
}
