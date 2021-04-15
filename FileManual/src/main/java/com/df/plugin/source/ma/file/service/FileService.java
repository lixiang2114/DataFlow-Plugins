package com.df.plugin.source.ma.file.service;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.df.plugin.source.ma.file.config.FileConfig;
import com.df.plugin.source.ma.file.config.ScanType;
import com.df.plugin.source.ma.file.handler.FileHandler;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.scheduler.SchedulerPool;

/**
 * @author Lixiang
 * @description 数据文件服务
 */
public class FileService {
	/**
	 * File配置
	 */
	private FileConfig fileConfig;
	
	/**
	 * 过滤器通道对象
	 */
	private Channel<String> filterChannel;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileService.class);
	
	public FileService(){}
	
	public FileService(FileConfig fileConfig){
		this.fileConfig=fileConfig;
	}
	
	/**
	 * 停止离线ETL流程
	 */
	public Boolean stopManualETLProcess(Object params) {
		log.info("stop manual ETL process...");
		fileConfig.flow.sourceStart=false;
		return true;
	}
	
	/**
	 * 启动离线ETL流程(读取预置数据文件)
	 * @param sourceToFilterChannel 下游通道对象
	 * @return 是否执行成功
	 * @throws Exception
	 */
	public Object startManualETLProcess(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("execute manual ETL process...");
		Boolean result=true;
		this.filterChannel=sourceToFilterChannel;
		if(fileConfig.scanType==ScanType.file) { //扫描单个文件
			log.info("hand single file...");
			result=disposeFile();
			try {
				if(result) {
					log.info("FileManual plugin manual etl process normal exit,execute checkpoint...");
				}else{
					log.info("FileManual plugin manual etl process error exit,execute checkpoint...");
				}
				fileConfig.refreshCheckPoint();
			} catch (IOException e) {
				log.error("FileManual plugin execute checkpoint occur error: {}",e);
			}
		}else{ //扫描指定目录
			File[] files=fileConfig.manualPath.listFiles();
			if(0==files.length) {
				log.info("there are no files to process in path: {}",fileConfig.manualPath);
				return true;
			}
			
			if(fileConfig.multiThread) { //异步并发递归扫描每个文件
				log.info("async hand multi file...");
				ThreadPoolTaskExecutor threadPool=SchedulerPool.getTaskExecutor();
				if(null==threadPool) {
					log.error("multiThread is true and unable to get spring thread pool...");
					throw new RuntimeException("multiThread is true and unable to get spring thread pool...");
				}
				asyncRead(fileConfig.manualPath,threadPool);
			}else{ //串行递归扫描每个文件
				log.info("sync hand multi file...");
				if(null!=fileConfig.manualFile && !fileConfig.readedFiles.contains(fileConfig.manualFile.getAbsolutePath())) {
					if(!disposeFile()) return false;
					fileConfig.readedFiles.add(fileConfig.manualFile.getAbsolutePath());
				}
				
				result=syncRead(fileConfig.manualPath);
				try {
					if(result) {
						log.info("FileManual plugin manual etl process normal exit,execute checkpoint...");
					}else{
						log.info("FileManual plugin manual etl process error exit,execute checkpoint...");
					}
					fileConfig.refreshCheckPoint();
				} catch (IOException e) {
					log.error("FileManual plugin execute checkpoint occur error: {}",e);
				}
			}
		}
		
		return result;
	}
	
	/**
	 * 同步扫描指定目录
	 * @param file 被扫描的文件对象
	 * @return 是否读取成功
	 * @throws Exception
	 */
	private boolean syncRead(File file) throws Exception {
		if(file.isFile()) {
			fileConfig.manualFile=file;
			fileConfig.lineNumber=0;
			fileConfig.byteNumber=0;
			
			log.info("starting dispose file: {}",fileConfig.manualFile.getAbsolutePath());
			Boolean flag=disposeFile();
			if(null!=flag && !flag) return false;
			
			log.info("finish dispose file: {}",fileConfig.manualFile.getAbsolutePath());
			fileConfig.readedFiles.add(fileConfig.manualFile.getAbsolutePath());
			return true;
		}
		
		File[] files=file.listFiles();
		for(File nextFile:files) {
			if(fileConfig.readedFiles.contains(nextFile.getAbsolutePath())) continue;
			if(!syncRead(nextFile)) return false;
		}
		
		return true;
	}
	
	/**
	 * 异步扫描指定目录
	 * @param file 被扫描的文件对象
	 * @return 是否读取成功
	 * @throws Exception
	 */
	private boolean asyncRead(File file,ThreadPoolTaskExecutor threadPool) throws Exception {
		if(file.isFile()) {
			threadPool.submit(new FileHandler(file,fileConfig,filterChannel));
			return true;
		}
		
		for(File nextFile:file.listFiles()) asyncRead(nextFile,threadPool);
		
		return true;
	}

	/**
	 * 扫描单个文件
	 */
	public boolean disposeFile() {
		boolean isNormal=false;
		RandomAccessFile raf=null;
		try{
			raf=new RandomAccessFile(fileConfig.manualFile, "r");
			if(0!=fileConfig.byteNumber)raf.seek(fileConfig.byteNumber);
			while(fileConfig.flow.sourceStart){
				String line=raf.readLine();
				if(null==line) break;
				
				fileConfig.lineNumber=fileConfig.lineNumber++;
				fileConfig.byteNumber=raf.getFilePointer();
				
				String record=line.trim();
				if(record.isEmpty()) continue;
				filterChannel.put(record);
			}
			
			isNormal=true;
		}catch(Exception e){
			isNormal=false;
			log.error("FileManual plugin dispose file occur error...",e);
		}finally{
			try{
				if(null!=raf) raf.close();
			}catch(IOException e){
				log.error("FileManual plugin close RandomAccessFile stream occur error...",e);
			}
		}
		
		return isNormal;
	}
}
