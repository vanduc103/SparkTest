package com.duclv.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SoccerAggregate {
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SoccerAggregate");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		//read all files from a directory
		JavaRDD<String> rdd = sc.textFile("file:///home/duclv/Downloads/soccer/ml/*.csv");
		JavaRDD<PlayerBallPossesion> possesionRdd = rdd.map(new Function<String, PlayerBallPossesion>() {
			@Override
			public PlayerBallPossesion call(String line) throws Exception {
				String[] items = line.split(",");
				PlayerBallPossesion possesion = new PlayerBallPossesion();
				possesion.setSid(Integer.parseInt(items[0]));
				possesion.setTime(Integer.parseInt(items[1]));
				possesion.setX(Integer.parseInt(items[2]));
				possesion.setY(Integer.parseInt(items[3]));
				possesion.setZ(Integer.parseInt(items[4]));
				return possesion;
			}
		});
		//create data frame
		SQLContext sqlContext = new SQLContext(sc);
	    DataFrame df = sqlContext.createDataFrame(possesionRdd, PlayerBallPossesion.class);
	    df = df.orderBy(df.col("time"));
	    //save to file
	    df.javaRDD().repartition(1).saveAsTextFile("file:///home/duclv/Downloads/soccer/result_aggregate");
		
		//close spark
		sc.close();
	}
}
