package com.theleapofcode.spark.core.example;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {

	// $SPARK_HOME/bin/spark-submit --class
	// com.theleapofcode.playground.spark.core.samples.WordCount
	// ./target/spark-1.0.jar ./target/classes/README.md
	public static void main(String[] args) {
		String fileName = "README.md";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = null;
		if (args != null && args.length > 0) {
			path = args[0];
		} else {
			path = ClassLoader.getSystemResource(fileName).getPath();
		}
		JavaRDD<String> logData = sc.textFile(path);

		// Flatmap split into words
		Map<String, Long> wordCount = logData.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).countByValue();
		wordCount.entrySet().forEach(entry -> System.out.println(entry.getKey() + " - " + entry.getValue()));
		
		sc.close();
	}

}
