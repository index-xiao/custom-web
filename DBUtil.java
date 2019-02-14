package com.jdbc.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

public class DBUtil {
	
	private static String driver;
	private static String url;
	private static String username;
	private static String password;
	
	private Connection conn = null;
	private PreparedStatement ps = null;
	private ResultSet res = null;

	public static DBUtil db = null;
	
	private DBUtil(String dbFileName) {
		setConnectProp(dbFileName);
	}
	
	public static DBUtil getInstance(String dbFileName) {
		if(db == null) {
			synchronized (DBUtil.class) {
				if(db == null) {
					db = new DBUtil(dbFileName);
				}
			}
		}
		return db;
	}
	
	/**
	 * 设置连接属性
	 * @param dbFileName
	 */
	private void setConnectProp(String dbFileName) {
		try {
			ResourceBundle resource = ResourceBundle.getBundle(dbFileName);
			driver = resource.getString("db.driver");
			url = resource.getString("db.url");
			username = resource.getString("db.username");
			password = resource.getString("db.password");
		}catch(Exception e) {
			System.out.println("文件名或属性不存在：" + e.getMessage());
		}
	}
	
	/**
	 * 获取链接
	 * @param dbFileName
	 * @return
	 */
	private Connection getConnecttion() throws SQLException{
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return DriverManager.getConnection(url, username, password);
	}
	
	/**
	 * 释放资源
	 */
	private void closeAll() {
		try {
			if(res != null) {
				res.close();
			}
			if(ps != null) {
				ps.close();
			}
			if(conn != null) {
				conn.close();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 更新操作
	 * @param sql
	 * @param params
	 * @return
	 */
	public int executeUpdate(String sql,Object... params) {
		int result = -1;
		try {
			execute(sql, params);  
			result = ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			closeAll();
		}
		
		return result;
	}

	private void execute(String sql, Object... params) throws SQLException {
		conn = this.getConnecttion();
		ps = conn.prepareStatement(sql);
		if (params != null) {    
		    for (int i = 0; i < params.length; i++) {    
		    	ps.setObject(i + 1, params[i]);    
		    }    
		}
	}
	
	/**
	 * 查询操作
	 * @param sql
	 * @param params
	 * @return
	 */
	public List<Map<String,Object>> executeQuery(String sql,Object... params) {
		List<Map<String,Object>> resultList = new ArrayList<>();
		try {
			execute(sql, params);  
			res = ps.executeQuery();
			ResultSetMetaData md = res.getMetaData();
			int count = md.getColumnCount();
			while(res.next()) {
				Map<String,Object> resultMap = new HashMap<>();
				for(int i = 1 ; i <= count; i++) {
					resultMap.put(md.getColumnLabel(i), res.getObject(i));   
				}
				resultList.add(resultMap);
			}
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			closeAll();
		}
		return resultList;
	}
}
