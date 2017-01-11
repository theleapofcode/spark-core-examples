package com.theleapofcode.spark.core.io;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SequenceFileExamples {

	public static void main(String[] args) {
		String fileName = "sequencefile.txt";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SequenceFile");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = null;
		if (args != null && args.length > 0) {
			path = args[0];
		} else {
			path = ClassLoader.getSystemResource(fileName).getPath();
		}

		// Load sequence file
		JavaPairRDD<Text, IntWritable> sequenceFileRDD = sc.sequenceFile(path, Text.class, IntWritable.class);
		JavaPairRDD<String, Integer> nativeTypesRDD = sequenceFileRDD
				.mapToPair(x -> new Tuple2<String, Integer>(x._1.toString(), x._2.get()));
		nativeTypesRDD.collect().forEach(System.out::println);

		// Save sequence file
		String savePath = "./save/sequencefile";
		File dir = new File(savePath);
		if (dir.exists()) {
			for (File f : dir.listFiles()) {
				f.delete();
			}
			dir.delete();
		}
		List<Tuple2<String, Integer>> input = new ArrayList<>();
		input.add(new Tuple2<String, Integer>("ONE", 1));
		input.add(new Tuple2<String, Integer>("TWO", 2));
		input.add(new Tuple2<String, Integer>("THREE", 3));
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
		JavaPairRDD<Text, IntWritable> result = rdd
				.mapToPair(x -> new Tuple2<Text, IntWritable>(new Text(x._1), new IntWritable(x._2)));
		result.saveAsHadoopFile(savePath, Text.class, IntWritable.class, SequenceFileOutputFormat.class);

		sc.close();
	}

}
