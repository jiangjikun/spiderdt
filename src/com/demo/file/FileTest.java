package com.demo.file;

import java.io.File;
import java.util.ArrayList;

public class FileTest {
	private static ArrayList<String> filelist = new ArrayList<String>();
	private static String path;
	private static ArrayList<String> finalPath = new ArrayList<String>();

	/*
	 * 通过递归得到某一路径下所有的目录及其文件
	 */
	public static ArrayList<String> getFiles(String filePath) {
		File root = new File(filePath);
		File[] files = root.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				/*
				 * 递归调用
				 */
				getFiles(file.getAbsolutePath());
				filelist.add(file.getAbsolutePath());
				path = file.getAbsolutePath();
			} else {

				path = file.getAbsolutePath();
			}
			if (path.endsWith(".schema")) {
				finalPath.add(path);
			}
		}
		return finalPath;
	}
}