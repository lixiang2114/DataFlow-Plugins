package com.df.plugin.sink.sql.dto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;

/**
 * @author Lixiang
 * @description SQL映射器
 */
public class SQLMapper {
	/**
	 * 插入预编译SQL语句
	 */
	private String insertSQL;
	
	/**
	 * 连接对象
	 */
	private Connection conn;
	
	/**
	 * 数据库表全名
	 */
	public String tableFullName;
	
	/**
	 * SQL预编译语句
	 */
	private PreparedStatement pstat;
	
	/**
	 * 字段类型映射字典
	 */
	public LinkedHashMap<String,Class<?>> fieldMap;
	
	public SQLMapper(String tableFullName,String insertSQL,LinkedHashMap<String,Class<?>> fieldMap,Connection conn) {
		this.tableFullName=tableFullName;
		this.insertSQL=insertSQL;
		this.fieldMap=fieldMap;
		this.conn=conn;
	}
	
	/**
	 * 获取插入执行语句
	 * @return 语句对象
	 */
	public PreparedStatement getStatement() {
		try {
			return (null==pstat || pstat.isClosed())?pstat=conn.prepareStatement(insertSQL):pstat;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int hashCode() {
		return tableFullName.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(this==obj) return true;
		if(!(obj instanceof SQLMapper)) return false;
		return tableFullName.equalsIgnoreCase(((SQLMapper)obj).tableFullName);
	}
}
