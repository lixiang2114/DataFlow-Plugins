package com.df.plugin.source.rt.file.service;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.rt.file.config.FileConfig;
import com.github.lixiang2114.flow.comps.Channel;

/**
 * @author Lixiang
 * @description 文件日志服务
 */
public class FileService {
	/**
	 * FileRealtime配置
	 */
	private FileConfig fileConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileService.class);
	
	public FileService(){}
	
	public FileService(FileConfig fileConfig){
		this.fileConfig=fileConfig;
	}
	
	/**
	 * 切换下一个日志文件
	 * @return 是否是最后一个日志文件
	 * @throws IOException 
	 */
	private void nextFile(RandomAccessFile randomFile) throws IOException{
		fileConfig.byteNumber=0;
		fileConfig.lineNumber=0;
		if(null!=randomFile) randomFile.close();
		String curFilePath=fileConfig.loggerFile.getAbsolutePath();
		
		if(fileConfig.delOnReaded)fileConfig.loggerFile.delete();
		int lastIndex=curFilePath.lastIndexOf(".");
		int newIndex=Integer.parseInt(curFilePath.substring(lastIndex+1))+1;
		fileConfig.loggerFile=new File(curFilePath.substring(0,lastIndex+1)+newIndex);
		log.info("FileRealtime switch buffer logFile to "+fileConfig.loggerFile.getAbsolutePath());
	}
	
	/**
	 * 是否读到最后一个日志文件
	 * @return 是否是最后一个日志文件
	 */
	private boolean isLastFile(){
		String curFilePath=fileConfig.loggerFile.getAbsolutePath();
		int curFileIndex=Integer.parseInt(curFilePath.substring(curFilePath.lastIndexOf(".")+1));
		for(String fileName:fileConfig.loggerFile.getParentFile().list()) if(curFileIndex<Integer.parseInt(fileName.substring(fileName.lastIndexOf(".")+1))) return false;
		return true;
	}
	
	/**
	 * 启动实时ETL流程(读取缓冲日志文件)
	 */
	public Object startRealtimeETLProcess(Channel<String> sourceToFilterChannel) {
		RandomAccessFile raf=null;
		log.info("sync execute ETL process...");
		try{
			out:while(fileConfig.flow.sourceStart){
				raf=new RandomAccessFile(fileConfig.loggerFile, "r");
				if(0!=fileConfig.byteNumber)raf.seek(fileConfig.byteNumber);
				while(fileConfig.flow.sourceStart){
					String line=raf.readLine();
					if(null==line) {
						if(!isLastFile()){
							nextFile(raf);
							break;
						}
						
						try{
							Thread.sleep(3000L);
						}catch(InterruptedException e){
							log.warn("source realtime ETL sleep interrupted,realtime ETL is over...");
							break out;
						}
						
						continue;
					}
					
					fileConfig.byteNumber=raf.getFilePointer();
					fileConfig.lineNumber=fileConfig.lineNumber++;
					
					String record=line.trim();
					if(record.isEmpty()) continue;
					
					sourceToFilterChannel.put(record);
				}
			}
			
			log.info("FileRealtime plugin realtime etl process normal exit,execute checkpoint...");
			return true;
		}catch(Exception e){
			log.error("FileRealtime plugin realtime etl process occur Error...",e);
			return false;
		}finally{
			try {
				fileConfig.refreshCheckPoint();
			} catch (IOException e) {
				log.error("FileRealtime plugin realtime refreshCheckPoint occur Error...",e);
			}
			try{
				if(null!=raf) raf.close();
			}catch(IOException e){
				log.error("FileRealtime plugin close random file stream occur Error...",e);
			}
		}
	}
	
	/**
	 * 停止实时ETL流程
	 */
	public Boolean stopRealtimeETLProcess(Object params) {
		log.info("stop realtime ETL process...");
		fileConfig.flow.sourceStart=false;
		return true;
	}
}
