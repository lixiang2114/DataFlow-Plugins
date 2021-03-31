package com.df.plugin.source.rt.file.config;

/**
 * @author Lixiang
 * @description 数据扫描模式
 */
public enum ScanMode {
	/**
	 * 文件扫描
	 */
	file("file"),
	
	/**
	 * 通道扫描
	 */
	channel("channel");
	
	public String name;
	
	private ScanMode(String name){
		this.name=name;
	}
}
