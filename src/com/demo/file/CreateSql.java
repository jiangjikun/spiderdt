package com.demo.file;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

public class CreateSql {
	private static HSSFSheet hssfsheet;
	private static String hsql;
	private static String tableName;
	private static ArrayList<String> Parts = new ArrayList<String>();
	private static String parter = "";

	@SuppressWarnings({ "deprecation", "resource" })
	public static ArrayList<String> CSql() throws Exception {
		InputStream is = new FileInputStream("BUS008000.xls");

		HSSFWorkbook hssfworkbook = new HSSFWorkbook(is);
		// sheet个数 hssfworkbook.getNumberOfSheets()
		// 遍历所有的sheet表
		for (int i = 0; i < 5; i++) {
			// 遍历sheet表
			hssfsheet = hssfworkbook.getSheetAt(i);
			// 获得sheet表的名字
			tableName = hssfworkbook.getSheetName(i);
			// 遍历每个sheet前，先给parter进行初始化
			parter = "";
			for (int j = 0; j < hssfworkbook.getSheetAt(i)
					.getPhysicalNumberOfRows() - 1; j++) {
				// 获取每个单元格内容
				String cell1 = hssfsheet.getRow(j).getCell((short) 0)
						.toString();

				String cell2 = hssfsheet.getRow(j).getCell((short) 1)
						.toString();

				String cellString = "    `" + cell1 + "` " + cell2 + ",\r\n";
				parter += cellString;
			}
			// 除去最后一个逗号
			String str = parter.substring(0, parter.length() - 3);

			hsql = " create external table if not exists ods.d_sample_"
					+ tableName + " (\n" + str + "\r\n)";
			Parts.add(hsql);
			// System.out.println(hsql);
		}
		return Parts;

	}
}