package com.df.plugin.sink.server.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.server.config.ContextConfig;

/**
 * @author Lixiang
 * @description 缓冲服务模块
 */
public class BufferService {
	/**
	 * 缓冲发送器配置
	 */
	private ContextConfig config;
	
	/**
	 * 本地缓冲文件输出流
	 */
	private BufferedWriter bufferWriter;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(BufferService.class);
	
	public BufferService() {}
	
	public BufferService(ContextConfig config) {
		this.config=config;
		try {
			this.bufferWriter=Files.newBufferedWriter(config.transferSaveFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		} catch (IOException e) {
			log.error("create transfer buffer stream occur error...",e);
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 写出数据到本地缓冲文件
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean writeBuffer(String message) throws Exception {
		try {
			//写入转存日志文件
			bufferWriter.write(message);
			bufferWriter.newLine();
			bufferWriter.flush();
			
			//当前转存日志文件未达到最大值则继续写转存日志文件
			if(config.transferSaveFile.length()<config.transferSaveMaxSize) return true;
			
			//当前转存日志文件达到最大值则增加转存日志文件
			String curTransSaveFilePath=config.transferSaveFile.getAbsolutePath();
			int lastIndex=curTransSaveFilePath.lastIndexOf(".");
			if(-1==lastIndex) {
				lastIndex=curTransSaveFilePath.length();
				curTransSaveFilePath=curTransSaveFilePath+".0";
			}
			
			config.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
			log.info("ServerSink switch transfer save buffer to: "+config.transferSaveFile.getAbsolutePath());
			
			//关闭原有文件流,同时创建新的文件流
			bufferWriter.close();
			bufferWriter=Files.newBufferedWriter(config.transferSaveFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
			
			return true;
		} catch (IOException e) {
			log.error("transfer save buffer running error...",e);
		}
		
		return false;
	}
	
	/**
	 * 关闭缓冲流
	 * @throws Exception
	 */
	public void stopBuffer() throws Exception {
		try{
			if(null!=bufferWriter) bufferWriter.close();
		}catch(IOException e){
			log.error("transfer save buffer close error...",e);
		}
	}
}
