package com.theleapofcode.spark.core.operation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDActionExamples {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDDActions");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 3)).cache();

		// Reduce
		Integer sum = rdd1.reduce((x, y) -> x + y);
		System.out.println("Sum of rdd1 - " + sum);

		// Fold
		Integer product = rdd1.fold(1, (x, y) -> x * y);
		System.out.println("Product of rdd1 - " + product);

		// Aggregate
		Tuple2<Integer, Integer> result = rdd1.aggregate(new Tuple2<Integer, Integer>(0, 0),
				(avg, val) -> new Tuple2<Integer, Integer>(avg._1 + val, avg._2 + 1),
				(avg1, avg2) -> new Tuple2<Integer, Integer>(avg1._1 + avg2._1, avg1._2 + avg2._2));
		System.out.println("Average od rdd1 - " + result._1 / result._2.doubleValue());

		// Collect
		List<Integer> values = rdd1.collect();
		System.out.println("Values of rdd1 - " + values);

		// Take
		List<Integer> vals = rdd1.take(2);
		System.out.println("First 2 values of rdd1 - " + vals);

		// Top
		List<Integer> top2 = rdd1.top(2);
		System.out.println("Top 2 values of rdd1 - " + top2);

		// Count
		long count = rdd1.count();
		System.out.println("Count of rdd1 - " + count);

		// CountByValue
		Map<Integer, Long> countByValue = rdd1.countByValue();
		System.out.println("Count by value of rdd1 - " + countByValue);

		sc.close();
	}

}
