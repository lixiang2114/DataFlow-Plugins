package com.df.plugin.transfer.amqp.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.amqp.config.AmqpConfig;
import com.df.plugin.transfer.amqp.util.AmqpUtil;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description AMQP服务模块
 */
public class AmqpService {
	/**
	 * AMQP客户端操作工具
	 */
	public AmqpUtil amqpUtil;
	
	/**
	 * AMQP配置
	 */
	private AmqpConfig amqpConfig;
	
	/**
	 * 转存缓冲输出器
	 */
	private BufferedWriter bufferedWriter;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(AmqpService.class);
	
	public AmqpService() {}
	
	public AmqpService(AmqpConfig amqpConfig) {
		this.amqpConfig=amqpConfig;
		this.amqpUtil=amqpConfig.amqpUtil;
		try {
			bufferedWriter=Files.newBufferedWriter(amqpConfig.transferSaveFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		} catch (IOException e) {
			log.error("transferSaveFile is not exists: ",e);
		}
	}
	
	/**
	 * 启动扫描器
	 * @throws Exception
	 */
	public void startScanner() throws Exception {
		while(amqpConfig.flow.transferStart) {
			for(String targetQueue:amqpConfig.targetQueues) {
				if(targetQueue.isEmpty()) continue;
				
				Object object=amqpUtil.syncRecv(targetQueue,amqpConfig.pollTimeoutMills);
				if(null==object) continue;
				
				String line=CommonUtil.toString(object);
				if(null==line || line.isEmpty()) continue;
	        	
	        	//写入转存日志文件
	        	bufferedWriter.write(line);
	        	bufferedWriter.newLine();
	        	bufferedWriter.flush();
	        	
	        	//当前转存日志文件未达到最大值则继续写转存日志文件
	        	if(amqpConfig.transferSaveFile.length()<amqpConfig.transferSaveMaxSize) continue;
	        	
	        	//当前转存日志文件达到最大值则增加转存日志文件
	        	String curTransSaveFilePath=amqpConfig.transferSaveFile.getAbsolutePath();
	        	int lastIndex=curTransSaveFilePath.lastIndexOf(".");
	        	if(-1==lastIndex) {
					lastIndex=curTransSaveFilePath.length();
					curTransSaveFilePath=curTransSaveFilePath+".0";
				}
	        	amqpConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
	        	log.info("AmqpTransfer switch transfer save log file to: "+amqpConfig.transferSaveFile.getAbsolutePath());
	        	
	        	try{
	        		bufferedWriter.close();
	        	}catch(IOException e){
	        		log.error("close transferSaveFile stream occur error: ",e);
	        	}
	        	
	        	bufferedWriter=Files.newBufferedWriter(amqpConfig.transferSaveFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,StandardOpenOption.APPEND);
			}
	    }
	}
}
