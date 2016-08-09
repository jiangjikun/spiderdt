package com.demo.file;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class ReadHdfsFile {
	private static Path path;
	private static String dir = "/user/hive/warehouse/ods.db/sample";
	private static String altSQL = "";
	private static String tableName = "";
	private static String sql = "";
	private static String csql = "";
	private static List<String> newParts = new ArrayList<String>();
	private static List<String> Parts = new ArrayList<String>();
	private static List<String> partitions = new ArrayList<String>();
	private static FileOutputStream fo;
	private static Integer partNum;
	private static String part1 = "";
	private static String part2 = "";
	private static String part3 = "";
	private static String jdbcdriver = "org.apache.hive.jdbc.HiveDriver";
	private static String jdbcurl = "jdbc:hive2://192.168.1.2:10000";
	private static String username = "spiderdt";
	private static String password = "spiderdt";
	private static Connection con;
	private static Statement st;
	private static ResultSet rs;

	public static void main(String[] args) throws Exception {
		tableName = "ods.d_" + args[0].replace(".", "_");
		// 解析传来的字段
		jie(args);
		// 创建表+分区
		cSql();
		// 遍历HDFS，得到加分区语句
		listFile(dir);
		// 执行建表、添加分区语句
		executeSql();
	}

	public static void jie(String[] args) throws Exception {
		String str = args[1].substring(1, args[1].length() - 1);
		String[] split = str.split(" ");
		partNum = split.length;
		System.out.println(partNum);
		if (partNum == 1) {
			part1 = split[0].substring(1);
		} else if (partNum == 2) {
			part1 = split[0].substring(1);
			part2 = split[1].substring(1);
		} else if (partNum == 3) {
			part1 = split[0].substring(1);
			part2 = split[1].substring(1);
			part3 = split[1].substring(1);
		}

		System.out.println(part1 + part2 + part3);
	}

	public static void cSql() throws Exception {
		String[] sf = tableName.split("_");
		System.out.println(tableName);

		ArrayList<String> cSql = CreateSql.CSql();
		for (String str : cSql) {
			String[] sp1 = str.split(" ");
			// System.out.println(str);
			// sp1[7]为表的名字
			// 一个分区
			System.out.println(sp1[7] + "%%%%%%%%%");
			System.out.println(sp1[7].equals(tableName));
			if (partNum == 1) {
				if (sp1[7].equals(tableName)) {
					String pix = "partitioned by (" + part1 + " string) \r\n "
							+ "LOCATION '/user/hive/warehouse/ods.db/" + sf[1]
							+ "/" + sf[2] + "' \r\n";
					csql = str + pix;
				}
				// 两个分区
			} else if (partNum == 2) {
				if (sp1[7].equals(tableName)) {
					String pix = "partitioned by (" + part1 + " string , "
							+ part2 + " string) \r\n"
							+ "LOCATION '/user/hive/warehouse/ods.db/" + sf[1]
							+ "/" + sf[2] + "' \r\n";
					csql = str + pix;
					System.out.println(csql);
				}
				// 三个分区
			} else if (partNum == 3) {
				if (sp1[7].equals(tableName)) {
					String pix = "partitioned by (" + part1 + " string , "
							+ part2 + " string" + part3 + " string) \r\n"
							+ "LOCATION '/user/hive/warehouse/ods.db/" + sf[1]
							+ "/" + sf[2] + "' \r\n";
					csql = str + pix;
				}
			}
			// System.out.println(csql);
		}
	}

	public static void listFile(String str) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.1.3:9000");
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> stats = fs.listFiles(new Path(str),
				true);

		while (stats.hasNext()) {
			LocatedFileStatus file = stats.next();

			if (file.isDirectory()) {
				path = file.getPath();
			} else {
				path = file.getPath();
				String ps = path.toString();
				String substring = ps.substring(ps.indexOf("sample"));
				String[] split = substring.split("\\/");
				if (partNum == 1) {
					String[] strings1 = split[2].split("=");
					// strings1[1]代表分区的时间，可以传入时间段，进行规定时间内查询

					altSQL = "alter table" + tableName + " add partition("
							+ strings1[0] + "='" + strings1[1] + "')";
					// System.out.println(altSQL);
				} else if (partNum == 2) {
					String[] strings1 = split[2].split("=");
					// strings1[1]代表分区的时间，可以传入时间段，进行规定时间内查询
					String[] strings2 = split[3].split("=");

					altSQL = "alter table " + tableName + " add partition("
							+ strings1[0] + "='" + strings1[1] + "' , "
							+ strings2[0] + "='" + strings2[1] + "')";
					// System.out.println(altSQL);
				} else if (partNum == 3) {
					String[] strings1 = split[2].split("=");
					// strings1[1]代表分区的时间，可以传入时间段，进行规定时间内查询
					String[] strings2 = split[3].split("=");
					String[] strings3 = split[4].split("=");

					altSQL = "alter table " + tableName + " add partition("
							+ strings1[0] + "='" + strings1[1] + "' , "
							+ strings2[0] + "='" + strings2[1] + "' , "
							+ strings3[0] + "='" + strings3[1] + "')";
					// System.out.println(altSQL);
				}
				// 给一段时间段数据加分区
				/*
				 * if (strings1[1].compareTo(args[0]) > 0 &&
				 * strings1[1].compareTo(args[1]) < 0) { newParts.add(altSQL); }
				 */
				newParts.add(altSQL);
			}
		}
		fs.close();
		File file1 = new File(
				"C:\\Users\\26924\\Desktop\\document\\partitions.txt");
		if (file1.exists()) {
			file1.delete();
		} // 输出
		fo = new FileOutputStream(
				"C:\\Users\\26924\\Desktop\\document\\partitions.txt");
		fo.write(csql.getBytes());
		List<String> showPart = showPart();
		for (String sql1 : newParts) {
			if (!showPart.contains(sql1)) {
				System.out.println(sql1);
				Parts.add(sql1);
				sql1 += "\r\n";
				fo.write(sql1.getBytes());
			}
		}
		System.out.println("END!!!");
		fo.close();
	}

	public static List<String> showPart() throws Exception {
		Class.forName(jdbcdriver);
		con = DriverManager.getConnection(jdbcurl, username, password);
		st = con.createStatement();
		sql = "show partitions " + tableName + "";

		rs = st.executeQuery(sql);
		if (rs != null) {
			while (rs.next()) {
				String part = rs.getString(1);
				String[] split = part.split("\\/");

				if (partNum == 1) {
					String[] strings1 = split[0].split("=");
					// strings1[1]代表分区的时间，可以传入时间段，进行规定时间内查询

					altSQL = "alter table " + tableName + " add partition("
							+ strings1[0] + "='" + strings1[1] + "')";
					// System.out.println(altSQL);
				} else if (partNum == 2) {
					String[] strings1 = split[0].split("=");
					// strings1[1]代表分区的时间，可以传入时间段，进行规定时间内查询
					String[] strings2 = split[1].split("=");

					altSQL = "alter table " + tableName + " add partition("
							+ strings1[0] + "='" + strings1[1] + "' , "
							+ strings2[0] + "='" + strings2[1] + "')";
					// System.out.println(altSQL);
				} else if (partNum == 3) {
					String[] strings1 = split[0].split("=");
					// strings1[1]代表分区的时间，可以传入时间段，进行规定时间内查询
					String[] strings2 = split[1].split("=");
					String[] strings3 = split[2].split("=");

					altSQL = "alter table " + tableName + " add partition("
							+ strings1[0] + "='" + strings1[1] + "' , "
							+ strings2[0] + "='" + strings2[1] + "' , "
							+ strings3[0] + "='" + strings3[1] + "')";
					// System.out.println(altSQL);
				}
				partitions.add(altSQL);
			}
			rs.close();
			st.close();
			con.close();
		} else {
			return null;
		}
		return partitions;

	}

	public static void executeSql() throws Exception {
		Class.forName(jdbcdriver);
		con = DriverManager.getConnection(jdbcurl, username, password);
		st = con.createStatement();

		st.executeUpdate(csql);

		for (String sql2 : Parts) {
			st.executeUpdate(sql2);
		}

		st.close();
		con.close();
	}
}
