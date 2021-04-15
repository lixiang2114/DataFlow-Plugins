package com.df.plugin.source.ma.hdfs.service;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.df.plugin.source.ma.hdfs.config.HdfsConfig;
import com.df.plugin.source.ma.hdfs.config.ScanType;
import com.df.plugin.source.ma.hdfs.handler.HdfsHandler;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.scheduler.SchedulerPool;

/**
 * @author Lixiang
 * @description HDFS分布式存储服务
 */
public class HdfsService {
	/**
	 * Hdfs分布式文件系统
	 */
	private FileSystem fileSystem;
	
	/**
	 * Hdfs分布式存储客户端配置
	 */
	private HdfsConfig hdfsConfig;
	
	/**
	 * 过滤器通道对象
	 */
	private Channel<String> filterChannel;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HdfsService.class);
	
	public HdfsService(){}
	
	public HdfsService(HdfsConfig hdfsConfig){
		this.hdfsConfig=hdfsConfig;
		this.fileSystem=hdfsConfig.fileSystem;
	}
	
	/**
	 * 停止离线ETL流程
	 */
	public Boolean stopManualETLProcess(Object params) {
		log.info("stop manual ETL process...");
		hdfsConfig.flow.sourceStart=false;
		return true;
	}
	
	/**
	 * 启动离线ETL流程(读取分布式存储文件)
	 * @param sourceToFilterChannel 下游通道对象
	 * @return 是否执行成功
	 * @throws Exception
	 */
	public Object startManualETLProcess(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("execute manual ETL process...");
		Boolean result=true;
		this.filterChannel=sourceToFilterChannel;
		if(hdfsConfig.scanType==ScanType.file) { //扫描单个文件
			log.info("hand single file...");
			result=disposeFile();
			try {
				if(result) {
					log.info("HdfsManual plugin manual etl process normal exit,execute checkpoint...");
				}else{
					log.info("HdfsManual plugin manual etl process error exit,execute checkpoint...");
				}
				hdfsConfig.refreshCheckPoint();
			} catch (IOException e) {
				log.error("HdfsManual plugin execute checkpoint occur error: {}",e);
			}
		}else{ //扫描指定目录
			RemoteIterator<LocatedFileStatus> files=fileSystem.listFiles(hdfsConfig.hdfsPath, true);
			if(hdfsConfig.multiThread) { //异步并发递归扫描每个文件
				log.info("async hand multi file...");
				ThreadPoolTaskExecutor threadPool=SchedulerPool.getTaskExecutor();
				if(null==threadPool) {
					log.error("multiThread is true and unable to get spring thread pool...");
					throw new RuntimeException("multiThread is true and unable to get spring thread pool...");
				}
				while (files.hasNext()) threadPool.submit(new HdfsHandler(files.next(),hdfsConfig,filterChannel));
			}else{ //串行递归扫描每个文件
				log.info("sync hand multi file...");
				if(null!=hdfsConfig.hdfsFile && !hdfsConfig.readedFileSet.contains(hdfsConfig.hdfsFile.toString())) { //串行递归时该文件是上次扫描的结束点
					if(!disposeFile()) return false;
					hdfsConfig.readedFileSet.add(hdfsConfig.hdfsFile.toString());
				}
				
				while (files.hasNext()) { //遍历出来只有文件,没有目录
					Path file=files.next().getPath();
					String fullPath=file.toString();
					if(hdfsConfig.readedFileSet.contains(fullPath)) continue;
					
					hdfsConfig.hdfsFile=file;
					hdfsConfig.lineNumber=0;
					hdfsConfig.byteNumber=0;
					
					log.info("starting dispose file: {}",fullPath);
					Boolean flag=disposeFile();
					if(null!=flag && !flag) return false;
					
					log.info("finish dispose file: {}",fullPath);
					hdfsConfig.readedFileSet.add(fullPath);
				}
			}
		}
		return result;
	}
	
	/**
	 * 扫描单个文件
	 */
	public boolean disposeFile() {
		boolean isNormal=false;
		FSDataInputStream fsdis=null;
		try{
			fsdis=fileSystem.open(hdfsConfig.hdfsFile,hdfsConfig.bufferSize);
			if(0!=hdfsConfig.byteNumber) fsdis.seek(hdfsConfig.byteNumber);
			while(hdfsConfig.flow.sourceStart && fsdis.available()>0){
				String line=fsdis.readUTF().trim();
				hdfsConfig.byteNumber=fsdis.getPos();
				hdfsConfig.lineNumber=hdfsConfig.lineNumber++;
				
				if(line.isEmpty()) continue;
				filterChannel.put(line);
			}
			
			isNormal=true;
		}catch(Exception e){
			isNormal=false;
			log.error("HdfsManual plugin dispose file occur error...",e);
		}finally{
			try{
				if(null!=fsdis) fsdis.close();
			}catch(IOException e){
				log.error("HdfsManual plugin close hdfs file stream occur error...",e);
			}
		}
		
		return isNormal;
	}
}
