package com.mangocity.dbutil.queryrunner;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import javax.sql.rowset.serial.SerialClob;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.mangocity.dbutil.domain.User;
import com.mangocity.dbutil.util.JdbcUtils;

/**
 * @ClassName: DBUtilsCRUDTest
 * @Description:使用dbutils框架的QueryRunner类完成CRUD,以及批处理
 * @author: 孤傲苍狼
 * @date: 2014-10-5 下午4:56:44
 *
 */
public class QueryRunnerCRUDTest {

	@Test
	public void add() throws SQLException {
		// 将数据源传递给QueryRunner，QueryRunner内部通过数据源获取数据库连接
		QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource());
		String sql = "insert into users(name,password,email,birthday) values(?,?,?,?)";
		Object params[] = { "孤傲苍狼", "123", "gacl@sina.com", new Date() };
		qr.update(sql, params);
	}

	@Test
	public void delete() throws SQLException {

		QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource());
		String sql = "delete from users where id=?";
		qr.update(sql, 1);

	}

	@Test
	public void update() throws SQLException {
		QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource());
		String sql = "update users set name=? where id=?";
		Object params[] = { "ddd", 5 };
		qr.update(sql, params);
	}

	@Test
	public void findUserById() throws SQLException {
		QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource());
		String sql = "select * from t_user u where u.user_id=?";
		User user = qr.query(sql, new ResultSetHandler<User>() {
			public User handle(ResultSet rs) throws SQLException {
				if (rs.next()) {
					User user = new User();
					user.setUserId(rs.getInt(1));
					user.setUserName(rs.getString("user_name"));
					user.setDesc(rs.getString("desc"));
					return user;
				}
				return null;
			}
		}, new Object[] { 1 });
		System.out.println("findUserById:" + user);
	}

	@Test
	public void findAllUser() throws SQLException {
		QueryRunner queryRunner = new QueryRunner(JdbcUtils.getDataSource());
		String sql = "select * from t_user";
		List<User> userList = queryRunner.query(sql,
				new ResultSetHandler<List<User>>() {
					public List<User> handle(ResultSet rs) throws SQLException {
						List<User> userList = Lists.newArrayList();
						while (rs.next()) {
							User user = new User();
							user.setUserId(rs.getInt(1));
							user.setUserName(rs.getString("user_name"));
							user.setDesc(rs.getString("desc"));
							userList.add(user);
						}
						return userList;
					}

				});
		System.out.println("userList: " + userList);
	}

	/**
	 * @Method: testBatch
	 * @Description:批处理
	 * @Anthor:孤傲苍狼
	 *
	 * @throws SQLException
	 */
	@Test
	public void testBatch() throws SQLException {
		QueryRunner qr = new QueryRunner(JdbcUtils.getDataSource());
		String sql = "insert into users(name,password,email,birthday) values(?,?,?,?)";
		Object params[][] = new Object[10][];
		for (int i = 0; i < 10; i++) {
			params[i] = new Object[] { "aa" + i, "123", "aa@sina.com",
					new Date() };
		}
		qr.batch(sql, params);
	}

	@Test
	public void testclob() throws SQLException, IOException {
		QueryRunner runner = new QueryRunner(JdbcUtils.getDataSource());
		String sql = "insert into testclob(resume) values(?)"; // clob
		// 这种方式获取的路径，其中的空格会被使用“%20”代替
		String path = QueryRunnerCRUDTest.class.getClassLoader()
				.getResource("data.txt").getPath();
		// 将“%20”替换回空格
		path = path.replaceAll("%20", " ");
		FileReader in = new FileReader(path);
		char[] buffer = new char[(int) new File(path).length()];
		in.read(buffer);
		SerialClob clob = new SerialClob(buffer);
		Object params[] = { clob };
		runner.update(sql, params);
	}
}
