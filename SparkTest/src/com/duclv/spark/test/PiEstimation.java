package com.duclv.spark.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class PiEstimation {
	public static void main(String[] args) {
		int NUM_SAMPLES = 100;
		List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
		for (int i = 0; i < NUM_SAMPLES; i++) {
			l.add(i);
		}
		
		SparkConf conf = new SparkConf().setAppName(PiEstimation.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
			public Boolean call(Integer i) {
				double x = Math.random();
				double y = Math.random();
				return x * x + y * y < 1;
			}
		}).count();
		System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
		while(true) {
			//loop forever
		}

	}
}
