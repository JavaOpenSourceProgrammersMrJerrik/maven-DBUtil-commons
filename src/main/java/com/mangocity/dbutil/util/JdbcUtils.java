package com.mangocity.dbutil.util;

import static com.mangocity.dbutil.util.PropertiesReaderUtils.getValue;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.google.common.primitives.Ints;
import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @ClassName: JdbcUtils
 * @Description: 数据库连接工具类
 * @author: Jerrik
 * @date: 2016-06-01 下午4:04:36
 */
public class JdbcUtils {

	private static ComboPooledDataSource ds = null;
	
	// 使用ThreadLocal存储当前线程中的Connection对象
	private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();

	// 在静态代码块中创建数据库连接池
	static {
		try {
			// 通过代码创建C3P0数据库连接池
			ds = new ComboPooledDataSource();
			ds.setDriverClass(getValue("driver_class.mysql"));
			ds.setJdbcUrl(getValue("url.mysql"));
			ds.setUser(getValue("username.mysql"));
			ds.setPassword(getValue("password.mysql"));
			ds.setInitialPoolSize(Ints.tryParse(getValue("pool.initialPoolSize")));
			ds.setMinPoolSize(Ints.tryParse(getValue("pool.minPoolSize")));
			ds.setMaxPoolSize(Ints.tryParse(getValue("pool.maxPoolSize")));
		} catch (Exception e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/**
	 * @Method: getConnection
	 * @Description: 从数据源中获取数据库连接
	 * @author: Jerrik
	 * @return Connection
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException {
		// 从当前线程中获取Connection
		Connection conn = threadLocal.get();
		if (conn == null) {
			conn = getDataSource().getConnection();
			threadLocal.set(conn);
		}
		return conn;
	}

	/**
	 * @Method: startTransaction
	 * @Description: 开启事务
	 * @author: Jerrik
	 *
	 */
	public static void startTransaction() {
		try {
			Connection conn = threadLocal.get();
			if (conn == null) {
				conn = getConnection();
				threadLocal.set(conn);
			}
			// 开启事务
			conn.setAutoCommit(false);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @Method: rollback
	 * @Description:回滚事务
	 * @author: Jerrik
	 *
	 */
	public static void rollback() {
		try {
			// 从当前线程中获取Connection
			Connection conn = threadLocal.get();
			if (conn != null) {
				conn.rollback();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @Method: commit
	 * @Description:提交事务
	 * @author: Jerrik
	 *
	 */
	public static void commit() {
		try {
			// 从当前线程中获取Connection
			Connection conn = threadLocal.get();
			if (conn != null) {
				conn.commit();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @Method: close
	 * @Description:关闭数据库连接(注意，并不是真的关闭，而是把连接还给数据库连接池)
	 * @author: Jerrik
	 *
	 */
	public static void close() {
		try {
			// 从当前线程中获取Connection
			Connection conn = threadLocal.get();
			if (conn != null) {
				conn.close();
				threadLocal.remove();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @Method: getDataSource
	 * @Description: 获取数据源
	 * @author: Jerrik
	 * @return DataSource
	 */
	public static DataSource getDataSource() {
		return ds;
	}
	
	public static void main(String[] args) throws SQLException {
		Connection conn = getDataSource().getConnection();
		System.out.println(conn);
	}
}