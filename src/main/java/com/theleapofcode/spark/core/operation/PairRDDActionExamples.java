package com.theleapofcode.spark.core.operation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRDDActionExamples {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("PairRDDActions");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<Integer, Integer> pairrdd = sc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(1, 2),
				new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6))).cache();
		System.out.println("Pair rdd - " + pairrdd.collect());

		// Count by key
		Map<Integer, Long> countByKey = pairrdd.countByKey();
		System.out.println("Pair rdd count by key - " + countByKey);

		// Collect as map
		Map<Integer, Integer> map = pairrdd.collectAsMap();
		System.out.println("Pair rdd as map - " + map);

		// lookup
		List<Integer> value = pairrdd.lookup(3);
		System.out.println("Pair rdd lookup value - " + value);

		sc.close();
	}

}
