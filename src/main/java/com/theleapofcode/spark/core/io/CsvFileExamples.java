package com.theleapofcode.spark.core.io;

import java.io.File;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import au.com.bytecode.opencsv.CSVReader;

public class CsvFileExamples {

	public static void main(String[] args) {
		String fileName = "users.csv";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("CsvFile");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = null;
		if (args != null && args.length > 0) {
			path = args[0];
		} else {
			path = ClassLoader.getSystemResource(fileName).getPath();
		}

		// Load CSV file
		JavaRDD<String> fileData = sc.textFile(path);
		JavaRDD<String[]> csvData = fileData.mapPartitions(lines -> {
			List<String[]> users = new LinkedList<>();
			while (lines.hasNext()) {
				String line = lines.next();
				try {
					CSVReader reader = new CSVReader(new StringReader(line));
					users.add(reader.readNext());
				} catch (Exception e) {
					// skip records on failure
				}
			}
			return users.iterator();
		});

		csvData.collect().forEach(csvLine -> {
			for (String csvField : csvLine) {
				System.out.println(csvField);
			}
		});

		// Save CSV file
		String savePath = "./save/csv";
		File dir = new File(savePath);
		if (dir.exists()) {
			for (File f : dir.listFiles()) {
				f.delete();
			}
			dir.delete();
		}
		fileData.saveAsTextFile(savePath);

		sc.close();
	}

}
