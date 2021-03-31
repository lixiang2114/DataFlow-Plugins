package com.df.plugin.source.ma.file.handler;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.ma.file.config.FileConfig;
import com.github.lixiang2114.flow.comps.Channel;

/**
 * @author Lixiang
 * @description 同步文件操作器
 */
public class FileHandler implements Callable<Boolean>{
	/**
	 * 扫描文件
	 */
	private File file;
	
	/**
	 * File配置
	 */
	private FileConfig fileConfig;
	
	/**
	 * 下游通道对象
	 */
	private Channel<String> filterChannel;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileHandler.class);
	
	public FileHandler(File file,FileConfig fileConfig,Channel<String> filterChannel) {
		this.file=file;
		this.fileConfig=fileConfig;
		this.filterChannel=filterChannel;
	}

	@Override
	public Boolean call() throws Exception {
		boolean isNormal=false;
		RandomAccessFile raf=null;
		try{
			raf=new RandomAccessFile(file, "r");
			while(fileConfig.flow.sourceStart){
				String line=raf.readLine();
				if(null==line) break;
				
				String record=line.trim();
				if(record.isEmpty()) continue;
				
				filterChannel.put(record);
			}
			
			isNormal=true;
			log.info("FileHandler hand file: {} normal exit,execute checkpoint...",file.getAbsolutePath());
		}catch(Exception e){
			isNormal=false;
			log.error("FileHandler hand file: {} occur error: {}",file.getAbsolutePath(),e);
		}finally{
			try{
				if(null!=raf) raf.close();
			}catch(IOException e){
				log.error("FileHandler hand close RandomAccessFile: {} stream occur error: {}",file.getAbsolutePath(),e);
			}
		}
		
		return isNormal;
	}
}
