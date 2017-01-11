package com.theleapofcode.spark.core.operation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTransformationExamples {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDDTransformations");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> rdd1 = sc
				.parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();
		JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("Han Solo", "Lando Calrissian")).cache();

		// Filter
		JavaRDD<String> rdd1Filtered = rdd1.filter(x -> x.contains("L"));
		System.out.println("Filtered rdd1 - " + rdd1Filtered.collect());

		// Map
		JavaRDD<String> rdd1Mapped = rdd1.map(x -> x.toUpperCase());
		System.out.println("Mapped rdd1 - " + rdd1Mapped.collect());

		// FlatMap
		JavaRDD<String> rdd1FlatMapped = rdd1.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		System.out.println("FlatMapped rdd1 - " + rdd1FlatMapped.collect());

		// Distinct
		JavaRDD<String> rdd1Distinct = rdd1.distinct();
		System.out.println("Distinct rdd1 - " + rdd1Distinct.collect());

		// Union
		JavaRDD<String> rdd1Unionrdd2 = rdd1.union(rdd2);
		System.out.println("Union of rdd1 and rdd2 - " + rdd1Unionrdd2.collect());

		// Intersection
		JavaRDD<String> rdd1Intersectionrdd2 = rdd1.intersection(rdd2);
		System.out.println("Intersection of rdd1 and rdd2 - " + rdd1Intersectionrdd2.collect());

		// Subtract
		JavaRDD<String> rdd1Minusrdd2 = rdd1.subtract(rdd2);
		System.out.println("Subtraction of rdd1 and rdd2 - " + rdd1Minusrdd2.collect());

		// Cartesian
		JavaPairRDD<String, String> rdd1Cartesianrdd2 = rdd1.cartesian(rdd2);
		System.out.println("Cartesian product of rdd1 and rdd2 - " + rdd1Cartesianrdd2.collect());

		sc.close();
	}

}
