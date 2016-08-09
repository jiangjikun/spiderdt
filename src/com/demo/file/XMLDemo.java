package com.demo.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLDemo {
	private static ArrayList<String> filelist = null;
	private static String path = "C:/Users/doris/Desktop/BUS008000";
	private static HSSFWorkbook hssfworkbook = null;

	public static void main(String[] args) throws Exception {
		// 打jar包时使用：
		/*
		 * InputStream is = new FileInputStream(args[0]); FileOutputStream of =
		 * new FileOutputStream(args[1]); filelist = FileTest.getFiles(is);
		 */

		filelist = FileTest.getFiles(path);
		for (String file : filelist) {
			read(file);
		}
		write(filelist);
	}

	public static void read(String file) throws Exception {
		System.out.println("File is :" + file);
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

		DocumentBuilder builder = dbf.newDocumentBuilder();
		InputStream in = new FileInputStream(file);
		Document doc = builder.parse(in);
		// root <analytic>
		Element root = doc.getDocumentElement();
		if (root == null)
			return;
		// all dataset node
		NodeList datasetNodes = root.getChildNodes();
		for (int i = 0; i < datasetNodes.getLength(); i++) {
			Node data = datasetNodes.item(i);
			if (data != null && data.getNodeType() == Node.ELEMENT_NODE) {
				// all field node
				NodeList fieldNodes = data.getChildNodes();
				if (fieldNodes == null)
					continue;
				for (int k = 0; k < fieldNodes.getLength(); k++) {
					Node field = fieldNodes.item(k);
					if (field != null
							&& field.getNodeType() == Node.ELEMENT_NODE) {
						System.out.print(field.getAttributes()
								.getNamedItem("flow").getNodeValue()
								+ " ");
						System.out.print(field.getAttributes()
								.getNamedItem("type").getNodeValue());
						System.out.println();
					}
				}
			}
		}
	}

	@SuppressWarnings("deprecation")
	public static void write(ArrayList<String> filelist) throws Exception {
		hssfworkbook = new HSSFWorkbook();
		for (int n = 0; n < filelist.size(); n++) {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

			DocumentBuilder builder = dbf.newDocumentBuilder();
			InputStream in = new FileInputStream(filelist.get(n));
			Document doc = builder.parse(in);
			// 工作簿

			// 创建sheet页
			HSSFSheet hssfsheet = hssfworkbook.createSheet();

			String s1 = filelist.get(n).substring(
					filelist.get(n).lastIndexOf("\\") + 1);
			String s2 = s1.substring(0, s1.indexOf("."));
			hssfworkbook.setSheetName(n, s2);
			// root <analytic>
			Element root = doc.getDocumentElement();
			if (root == null)
				return;
			// System.out.println(root.getAttribute("name"));
			// all dataset node
			NodeList datasetNodes = root.getChildNodes();
			for (int i = 0; i < datasetNodes.getLength(); i++) {
				Node data = datasetNodes.item(i);
				if (data != null && data.getNodeType() == Node.ELEMENT_NODE) {
					// all field node
					NodeList fieldNodes = data.getChildNodes();
					if (fieldNodes == null)
						continue;
					for (int k = 0; k < fieldNodes.getLength(); k++) {

						HSSFRow hssfrow = hssfsheet.createRow(k / 2);
						Node field = fieldNodes.item(k);
						if (field != null
								&& field.getNodeType() == Node.ELEMENT_NODE) {
							hssfrow.createCell((short) 0).setCellValue(
									field.getAttributes().getNamedItem("flow")
											.getNodeValue());
							hssfrow.createCell((short) 1).setCellValue(
									field.getAttributes().getNamedItem("type")
											.getNodeValue());
						}
					}
				}
			}
		}
		File file1 = new File("C:/Users/doris/Desktop/BUS008000.xls");
		if (file1.exists()) {
			file1.delete();
		} // 输出
		FileOutputStream fileoutputstream = new FileOutputStream(
				"C:/Users/doris/Desktop/BUS008000.xls");
		hssfworkbook.write(fileoutputstream);
		fileoutputstream.close();
	}

}
