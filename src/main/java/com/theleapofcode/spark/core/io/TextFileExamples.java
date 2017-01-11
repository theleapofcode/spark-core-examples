package com.theleapofcode.spark.core.io;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TextFileExamples {

	public static void main(String[] args) {
		String fileName = "README.md";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("TextFile");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = null;
		if (args != null && args.length > 0) {
			path = args[0];
		} else {
			path = ClassLoader.getSystemResource(fileName).getPath();
		}

		// Load text file
		JavaRDD<String> fileData = sc.textFile(path).cache();
		fileData.collect().forEach(System.out::println);

		// Save text file
		String savePath = "./save/README";
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
