package com.theleapofcode.spark.core.operation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class PairRDDTransformationExamples {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("PairRDDTransformations");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> rdd1 = sc.parallelize(
				Arrays.asList("I love you", "I know", "May the force be with you", "I have a bad feeling about this"))
				.cache();

		// MapToPair
		JavaPairRDD<String, String> pairrdd = rdd1.mapToPair(x -> new Tuple2<String, String>(x.split(" ")[0], x))
				.cache();
		System.out.println("Pair from rdd - " + pairrdd.collect());

		// Parallelize Pairs
		JavaPairRDD<Integer, Integer> pairrdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(1, 2),
				new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6))).cache();
		System.out.println("Pair rdd1 - " + pairrdd1.collect());
		JavaPairRDD<Integer, Integer> pairrdd2 = sc
				.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(3, 7), new Tuple2<Integer, Integer>(4, 8)))
				.cache();
		System.out.println("Pair rdd2 - " + pairrdd2.collect());

		// Reduce by key
		JavaPairRDD<Integer, Integer> pairrdd1ReducedByKey = pairrdd1.reduceByKey((x, y) -> x + y);
		System.out.println("Pair rdd1 reduced by key - " + pairrdd1ReducedByKey.collect());

		// Fold by key
		JavaPairRDD<Integer, Integer> pairrdd1FoldedByKey = pairrdd1.foldByKey(0, (x, y) -> x + y);
		System.out.println("Pair rdd1 folded by key - " + pairrdd1FoldedByKey.collect());

		// Group by key
		JavaPairRDD<Integer, Iterable<Integer>> pairrdd1GroupedByKey = pairrdd1.groupByKey();
		System.out.println("Pair rdd1 grouped by key - " + pairrdd1GroupedByKey.collect());

		// Map values
		JavaPairRDD<Integer, Integer> pairrdd1MapValues = pairrdd1.mapValues(x -> x + 1);
		System.out.println("Pair rdd map values - " + pairrdd1MapValues.collect());

		// FlatMap values
		JavaPairRDD<String, String> pairrddFaltMapValues = pairrdd.flatMapValues(x -> Arrays.asList(x.split(" ")));
		System.out.println("Pair rdd flat map values - " + pairrddFaltMapValues.collect());

		// Keys
		JavaRDD<Integer> keys = pairrdd1.keys();
		System.out.println("Pair rdd keys - " + keys.collect());

		// Values
		JavaRDD<Integer> values = pairrdd1.values();
		System.out.println("Pair rdd values - " + values.collect());

		// Sort by key
		JavaPairRDD<Integer, Integer> pairrdd1SortedAscending = pairrdd1.sortByKey(true);
		System.out.println("Pair rdd sorted by key ascending - " + pairrdd1SortedAscending.collect());
		JavaPairRDD<Integer, Integer> pairrdd1SortedDescending = pairrdd1.sortByKey(false);
		System.out.println("Pair rdd sorted by key descending - " + pairrdd1SortedDescending.collect());

		// Subtract by key
		JavaPairRDD<Integer, Integer> pairrdd1Subtracted = pairrdd1.subtractByKey(pairrdd2);
		System.out.println("Pair rdd1 and rdd2 subtracted - " + pairrdd1Subtracted.collect());

		// Inner Join
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> pairrdd1InnerJoined = pairrdd1.join(pairrdd2);
		System.out.println("Pair rdd1 and rdd2 inner joined - " + pairrdd1InnerJoined.collect());

		// Right Outer Join
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> pairrdd1RightOuterJoined = pairrdd1
				.rightOuterJoin(pairrdd2);
		System.out.println("Pair rdd1 and rdd2 right outer joined - " + pairrdd1RightOuterJoined.collect());

		// Left Outer Join
		JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> pairrdd1LeftOuterJoined = pairrdd1
				.leftOuterJoin(pairrdd2);
		System.out.println("Pair rdd1 and rdd2 left outer joined - " + pairrdd1LeftOuterJoined.collect());

		// CoGroup
		JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> pairrdd1CoGrouped = pairrdd1
				.cogroup(pairrdd2);
		System.out.println("Pair rdd1 and rdd2 co grouped - " + pairrdd1CoGrouped.collect());

		// Filter
		JavaPairRDD<Integer, Integer> pairrdd1filtered = pairrdd1.filter(x -> x._2 > 2);
		System.out.println("Pair rdd1 filtered - " + pairrdd1filtered.collect());

		// Combine by key
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> resultrdd = pairrdd1.combineByKey(
				x -> new Tuple2<Integer, Integer>(x, 1),
				(acc, val) -> new Tuple2<Integer, Integer>(acc._1 + val, acc._2 + 1),
				(acc1, acc2) -> new Tuple2<Integer, Integer>(acc1._1 + acc2._1$mcC$sp(), acc1._2 + acc2._2));
		JavaPairRDD<Integer, Double> avgrdd = resultrdd.mapValues(x -> x._1 / x._2.doubleValue());
		System.out.println("Average of rdd1 by key - " + avgrdd.collect());

		sc.close();
	}

}
