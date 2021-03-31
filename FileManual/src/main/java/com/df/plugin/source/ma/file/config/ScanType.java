package com.df.plugin.source.ma.file.config;

/**
 * @author Lixiang
 * @description 扫描类型
 */
public enum ScanType {
	/**
	 * 文件
	 */
	file("file"),
	
	/**
	 * 目录
	 */
	path("path");
	
	public String type;
	
	private ScanType(String type){
		this.type=type;
	}
}
