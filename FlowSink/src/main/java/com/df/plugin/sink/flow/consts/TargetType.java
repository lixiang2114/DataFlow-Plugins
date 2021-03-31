package com.df.plugin.sink.flow.consts;

/**
 * @author Lixiang
 * @description 发送目标类型
 */
public enum TargetType {
	/**
	 * 发送到文件
	 */
	file("file"),
	
	/**
	 * 发送到通道
	 */
	channel("channel"),
	
	/**
	 * 发送到缓冲
	 */
	buffer("buffer","file"),
	
	/**
	 * 发送到流程
	 */
	flow("flow","channel");
	
	/**
	 * 目标介质类型名
	 */
	public String name;
	
	/**
	 * 目标介质父级类型名
	 */
	public String parentName;
	
	public String getSuperName() {
		return null==parentName?name:parentName;
	}
	
	public TargetType getSuperType() {
		return TargetType.valueOf(getSuperName());
	}
	
	private TargetType(String name) {
		this(name,null);
	}
	
	private TargetType(String name,String parentName) {
		this.name=name;
		this.parentName=parentName;
	}
}
