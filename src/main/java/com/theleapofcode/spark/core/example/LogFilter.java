package com.theleapofcode.spark.core.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LogFilter {

	public static void main(String[] args) {
		String fileName = "app.log";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("LogFilter");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = null;
		if (args != null && args.length > 0) {
			path = args[0];
		} else {
			path = ClassLoader.getSystemResource(fileName).getPath();
		}
		JavaRDD<String> logData = sc.textFile(path).cache();

		// Filter logs with error and warn
		JavaRDD<String> badData = logData.filter(s -> s.contains("ERROR") || s.contains("WARN")).cache();

		System.out.println("Lines with ERROR or WARN - " + badData.count());
		badData.collect().forEach(System.out::println);
		sc.close();
	}

}
