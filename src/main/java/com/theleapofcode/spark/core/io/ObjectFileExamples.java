package com.theleapofcode.spark.core.io;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ObjectFileExamples {

	public static void main(String[] args) {
		String fileName = "objectfile.txt";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("ObjectFile");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = null;
		if (args != null && args.length > 0) {
			path = args[0];
		} else {
			path = ClassLoader.getSystemResource(fileName).getPath();
		}

		// Load object file
		JavaRDD<Object> objectFileRDD = sc.objectFile(path);
		JavaRDD<User> userRDD = objectFileRDD.map(x -> (User) x);
		userRDD.collect().forEach(System.out::println);

		// Save object file
		String savePath = "./save/objectfile";
		File dir = new File(savePath);
		if (dir.exists()) {
			for (File f : dir.listFiles()) {
				f.delete();
			}
			dir.delete();
		}
		List<User> input = new ArrayList<>();
		input.add(new User(1, "Tony", "Stark", "tstark@marvel.com"));
		input.add(new User(2, "Steve", "Rogers", "srogers@marvel.com"));
		JavaRDD<User> rdd = sc.parallelize(input);
		rdd.saveAsObjectFile(savePath);

		sc.close();
	}

}
