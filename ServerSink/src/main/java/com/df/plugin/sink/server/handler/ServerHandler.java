package com.df.plugin.sink.server.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.netty.Server;

/**
 * @author Lixiang
 * @description 发送服务句柄
 */
public class ServerHandler implements Runnable{
	/**
	 * 嵌入式服务器
	 */
	private Server server;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ServerHandler.class);
	
	public ServerHandler(Server server) {
		this.server=server;
	}

	@Override
	public void run() {
		try {
			server.startServer();
		} catch (Exception e) {
			log.error("start sink server occur error...",e);
			throw new RuntimeException(e);
		}
	}
}
