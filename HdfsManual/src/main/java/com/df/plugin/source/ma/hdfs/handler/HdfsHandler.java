package com.df.plugin.source.ma.hdfs.handler;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.ma.hdfs.config.HdfsConfig;
import com.github.lixiang2114.flow.comps.Channel;

/**
 * @author Lixiang
 * @description 异步文件操作器
 */
public class HdfsHandler implements Callable<Boolean>{
	/**
	 * 扫描文件
	 */
	private Path file;
	
	/**
	 * Hdfs分布式文件系统
	 */
	private FileSystem fileSystem;
	
	/**
	 * Hdfs分布式存储客户端配置
	 */
	private HdfsConfig hdfsConfig;
	
	/**
	 * 下游通道对象
	 */
	private Channel<String> filterChannel;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HdfsHandler.class);
	
	public HdfsHandler(LocatedFileStatus fileStatus,HdfsConfig hdfsConfig,Channel<String> filterChannel) {
		this.file=fileStatus.getPath();
		this.hdfsConfig=hdfsConfig;
		this.filterChannel=filterChannel;
		this.fileSystem=hdfsConfig.fileSystem;
	}

	@Override
	public Boolean call() throws Exception {
		boolean isNormal=false;
		FSDataInputStream fsdis=null;
		try{
			fsdis=fileSystem.open(file,hdfsConfig.bufferSize);
			while(hdfsConfig.flow.sourceStart && fsdis.available()>0){
				String line=fsdis.readUTF().trim();
				if(line.isEmpty()) continue;
				filterChannel.put(line);
			}
			
			isNormal=true;
			log.info("HdfsHandler hand file: {} normal exit,execute checkpoint...",file.toString());
		}catch(Exception e){
			isNormal=false;
			log.error("HdfsHandler hand file: {} occur error: {}",file.toString(),e);
		}finally{
			try{
				if(null!=fsdis) fsdis.close();
			}catch(IOException e){
				log.error("HdfsHandler close hdfs file stream: {} occur error: {}",file.toString(),e);
			}
		}
		
		return isNormal;
	}
}
