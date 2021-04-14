package com.df.plugin.sink.hdfs.service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.hdfs.config.HdfsConfig;

/**
 * @author Lixiang
 * @description Hdfs服务
 */
public class HdfsService {
	/**
	 * 分布式文件系统
	 */
	private FileSystem fileSystem;
	
	/**
	 * Hdfs发送器配置
	 */
	private HdfsConfig hdfsConfig;
	
	/**
	 * 本地批处理缓冲文件输出流
	 */
	private BufferedWriter batchFileWriter;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HdfsService.class);
	
	public HdfsService(){}
	
	public HdfsService(HdfsConfig hdfsConfig){
		this.hdfsConfig=hdfsConfig;
		this.fileSystem=hdfsConfig.fileSystem;
		try {
			this.batchFileWriter=Files.newBufferedWriter(hdfsConfig.batchFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		} catch (IOException e) {
			log.error("create local batch buffer stream occur error...",e);
			throw new RuntimeException("create local batch buffer stream occur error...",e);
		}
	}
	
	/**
	 * 写出数据到本地批处理文件
	 * @param message 消息参数
	 * @throws Exception 
	 */
	public boolean writeMessage(String message) throws Exception {
		if(null==message) {
			if(0==hdfsConfig.batchFile.length()) return true;
			return batchSendToHdfs();
		}
		
		if((message=message.trim()).isEmpty()) return true;
		
		try {
			batchFileWriter.write(message);
			batchFileWriter.newLine();
			batchFileWriter.flush();
			
			if(hdfsConfig.maxBatchBytes>hdfsConfig.batchFile.length()) return true;
			return batchSendToHdfs();
		} catch (IOException e) {
			log.error("write message occur error: ",e);
		}
		return false;
	}
	
	/**
	 * 批量推送数据到分布式存储系统
	 * @throws IOException
	 */
	private boolean batchSendToHdfs() throws IOException {
		FSDataOutputStream hdfsFileStream=null;
		if(fileSystem.exists(hdfsConfig.hdfsFile)){
			hdfsFileStream=fileSystem.append(hdfsConfig.hdfsFile,hdfsConfig.bufferSize);
		}else{
			hdfsFileStream=fileSystem.create(hdfsConfig.hdfsFile, true,hdfsConfig.bufferSize);
		}
		
		List<String> lines=null;
		try{
			int len=(lines=Files.readAllLines(hdfsConfig.batchFile.toPath())).size();
			for(int i=0;i<len;hdfsFileStream.writeUTF(lines.get(i++)+"\n"));
			hdfsFileStream.flush();
		}finally{
			if(null!=hdfsFileStream) hdfsFileStream.close();
		}
		
		batchFileWriter.close();
		batchFileWriter=Files.newBufferedWriter(hdfsConfig.batchFile.toPath(), StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING);
		
		if(hdfsConfig.maxFileSize>fileSystem.getFileStatus(hdfsConfig.hdfsFile).getLen()) return true;
		String hdfsPathStr=hdfsConfig.hdfsFile.toString();
		int lastIndex=hdfsPathStr.lastIndexOf(".");
		
		hdfsConfig.hdfsFile=new Path(hdfsPathStr.substring(0,lastIndex+1)+(Integer.parseInt(hdfsPathStr.substring(lastIndex+1))+1));
		log.info("hdfsFile switch to: "+hdfsConfig.hdfsFile.toString());
		
		return true;
	}
	
	/**
	 * 停止本地批量缓冲文件流进程
	 * @throws IOException
	 */
	public void stop() throws IOException {
		if(null!=batchFileWriter) batchFileWriter.close();
	}
}
