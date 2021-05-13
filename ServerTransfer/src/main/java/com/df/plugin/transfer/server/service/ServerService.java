package com.df.plugin.transfer.server.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.server.config.ServerConfig;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.util.RestClient;
import com.github.lixiang2114.netty.TcpClient;

/**
 * @author Lixiang
 * @description 第三方服务器服务模块
 */
public class ServerService {
	/**
	 * TCP客户端
	 */
	private TcpClient tcpClient;
	
	/**
	 * ServerSource配置
	 */
	private ServerConfig serverConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ServerService.class);
	
	public ServerService(){}
	
	public ServerService(ServerConfig serverConfig){
		this.serverConfig=serverConfig;
		if(!"tcp".equals(serverConfig.protocol)) return;
		this.tcpClient=new TcpClient(serverConfig.host,serverConfig.port);
	}
	
	/**
	 * 启动转存服务
	 * @param transferToETLChannel
	 * @return 服务是否执行成功
	 */
	public Object startTransferSave(Channel<String> transferToETLChannel) {
		if("tcp".equals(serverConfig.protocol)) {
			return startTcpTransferSave(transferToETLChannel);
		}else{
			return startHttpTransferSave(transferToETLChannel);
		}
	}

	/**
	 * 启动TCP转存服务
	 * @param transferToETLChannel
	 * @return 服务是否执行成功
	 */
	private Object startTcpTransferSave(Channel<String> transferToETLChannel) {
		BufferedWriter bw=null;
		log.info("start transfer save process...");
		
		try {
			String line=null;
			tcpClient.connect();
			while(serverConfig.flow.transferStart) {
				bw=Files.newBufferedWriter(serverConfig.transferSaveFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
				while(serverConfig.flow.transferStart) {
					if(null==(line=tcpClient.getMessage(3000L))) continue;
					if((line=line.trim()).isEmpty()) continue;
					
					//写入转存文件
					bw.write(line);
					bw.newLine();
					bw.flush();
					
					//当前转存文件未达到最大值则继续写转存文件
					if(serverConfig.transferSaveFile.length()<serverConfig.transferSaveMaxSize) continue;
					
					//当前转存文件达到最大值则增加转存文件
					String curTransSaveFilePath=serverConfig.transferSaveFile.getAbsolutePath();
					int lastIndex=curTransSaveFilePath.lastIndexOf(".");
					if(-1==lastIndex) {
						lastIndex=curTransSaveFilePath.length();
						curTransSaveFilePath=curTransSaveFilePath+".0";
					}
					
					serverConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
					log.info("ServerTransfer switch transfer save log file to: "+serverConfig.transferSaveFile.getAbsolutePath());
					bw.close();
					break;
				}
			}
		} catch (Exception e) {
			log.error("transfer save process running error...",e);
		}finally{
			try{
				if(null!=bw) bw.close();
			}catch(IOException e){
				log.error("transfer save process close error...",e);
			}
		}
		
		return true;
	}
	
	/**
	 * 启动HTTP转存服务
	 * @param transferToETLChannel
	 * @return 服务是否执行成功
	 */
	private Object startHttpTransferSave(Channel<String> transferToETLChannel) {
		BufferedWriter bw=null;
		log.info("start transfer save process...");
		
		try {
			String line=null;
			while(serverConfig.flow.transferStart) {
				bw=Files.newBufferedWriter(serverConfig.transferSaveFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
				while(serverConfig.flow.transferStart) {
					Thread.sleep(serverConfig.httpPullIntervalMills);
					if(null==(line=RestClient.get(serverConfig.connStr).getBody())) continue;
					if((line=line.trim()).isEmpty()) continue;
					
					//写入转存文件
					bw.write(line);
					bw.newLine();
					bw.flush();
					
					//当前转存文件未达到最大值则继续写转存文件
					if(serverConfig.transferSaveFile.length()<serverConfig.transferSaveMaxSize) continue;
					
					//当前转存文件达到最大值则增加转存文件
					String curTransSaveFilePath=serverConfig.transferSaveFile.getAbsolutePath();
					int lastIndex=curTransSaveFilePath.lastIndexOf(".");
					if(-1==lastIndex) {
						lastIndex=curTransSaveFilePath.length();
						curTransSaveFilePath=curTransSaveFilePath+".0";
					}
					
					serverConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
					log.info("ServerTransfer switch transfer save log file to: "+serverConfig.transferSaveFile.getAbsolutePath());
					bw.close();
					break;
				}
			}
		} catch (Exception e) {
			log.error("transfer save process running error...",e);
		}finally{
			try{
				if(null!=bw) bw.close();
			}catch(IOException e){
				log.error("transfer save process close error...",e);
			}
		}
		
		return true;
	}
	
	/**
	 * 停止转存日志服务
	 */
	public Boolean stopTransferSave(Object params) {
		log.info("stop transfer save process...");
		serverConfig.flow.transferStart=false;
		if(null!=tcpClient) tcpClient.destory();
		return true;
	}
}
