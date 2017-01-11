package com.theleapofcode.spark.core.io;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonFileExamples {

	public static void main(String[] args) {
		String fileName = "users.json";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("JsonFile");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = null;
		if (args != null && args.length > 0) {
			path = args[0];
		} else {
			path = ClassLoader.getSystemResource(fileName).getPath();
		}

		// Load JSON file
		JavaRDD<String> fileData = sc.textFile(path);
		JavaRDD<User> jsonData = fileData.mapPartitions(lines -> {
			List<User> users = new LinkedList<>();
			ObjectMapper mapper = new ObjectMapper();
			while (lines.hasNext()) {
				String line = lines.next();
				try {
					users.add(mapper.readValue(line, User.class));
				} catch (Exception e) {
					// skip records on failure
				}
			}
			return users.iterator();
		});

		System.out.println("Json data - " + jsonData.collect());

		// Save JSON file
		String savePath = "./save/json";
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
