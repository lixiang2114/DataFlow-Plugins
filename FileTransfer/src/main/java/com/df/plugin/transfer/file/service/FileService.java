package com.df.plugin.transfer.file.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.file.config.FileConfig;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.util.ApplicationUtil;
import com.github.lixiang2114.flow.util.FileUtil;

/**
 * @author Lixiang
 * @description 文件日志服务
 */
public class FileService {
	/**
	 * JVM系统运行时
	 */
	private Runtime runtime;
	
	/**
	 * FileSource配置
	 */
	private FileConfig fileConfig;
	
	/**
	 * Tailf子进程
	 */
	private Process tailfSubProcess;
	
	/**
	 * Tailf启动命令
	 */
	private String tailfStartCommand;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileService.class);
	
	public FileService(){}
	
	public FileService(FileConfig fileConfig){
		this.fileConfig=fileConfig;
		runtime=Runtime.getRuntime();
		
		if(FileConfig.isWin){
			tailfStartCommand=FileConfig.winTailfCmd+" "+fileConfig.appLogFile;
		}else{
			tailfStartCommand="tail -F "+fileConfig.appLogFile;
		}
		
		log.info("tailf start command is: "+tailfStartCommand);
	}

	/**
	 * 启动转存日志服务
	 */
	public Object startLogTransferSave(Channel<String> transferToETLChannel) {
		BufferedWriter bw=null;
		LineNumberReader lnr=null;
		log.info("start transfer save process...");
		
		try {
			String line=null;
			tailfSubProcess=runtime.exec(tailfStartCommand);
			Long tailfPid=ApplicationUtil.getProcessID(tailfSubProcess);
			if(null!=tailfPid) FileUtil.overrideWriteFile(fileConfig.tailPidPath, tailfPid.toString());
			lnr=new LineNumberReader(new InputStreamReader(tailfSubProcess.getInputStream()));
			
			out:while(fileConfig.flow.transferStart){
				bw=Files.newBufferedWriter(fileConfig.transferSaveFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
				while(fileConfig.flow.transferStart){
					if(null==(line=lnr.readLine())){
						try{
							Thread.sleep(1000L);
						}catch(InterruptedException e){
							log.warn("transfer Save sleep interrupted,transfer is over...");
							break out;
						}
						
						continue;
					}
					
					//写入转存日志文件
					bw.write(line);
					bw.newLine();
					bw.flush();
					
					//当前转存日志文件未达到最大值则继续写转存日志文件
					if(fileConfig.transferSaveFile.length()<fileConfig.transferSaveMaxSize) continue;
					
					//当前转存日志文件达到最大值则增加转存日志文件
					String curTransSaveFilePath=fileConfig.transferSaveFile.getAbsolutePath();
					int lastIndex=curTransSaveFilePath.lastIndexOf(".");
					if(-1==lastIndex) {
						lastIndex=curTransSaveFilePath.length();
						curTransSaveFilePath=curTransSaveFilePath+".0";
					}
					
					fileConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
					log.info("FileTransfer switch transfer save log file to: "+fileConfig.transferSaveFile.getAbsolutePath());
					bw.close();
					break;
				}
			}
		} catch (Exception e) {
			log.error("transfer save process running error...",e);
		}finally{
			try{
				if(null!=bw) bw.close();
				if(null!=lnr) lnr.close();
			}catch(IOException e){
				log.error("transfer save process close error...",e);
			}
		}
		
		return true;
	}
	
	/**
	 * 停止转存日志服务
	 */
	public Boolean stopLogTransferSave(Object params) {
		log.info("stop transfer save process...");
		if(!tailfSubProcess.isAlive()) return true;
		fileConfig.flow.transferStart=false;
		Boolean sucKilled=ApplicationUtil.forceKillProcess(tailfSubProcess);
		if(null!=sucKilled && sucKilled) fileConfig.tailPidPath.delete();
		return tailfSubProcess.isAlive();
	}
}
