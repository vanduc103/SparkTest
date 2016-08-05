package com.duclv.spark.test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.net.util.Base64;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import scala.collection.mutable.StringBuilder;

public class WifiStreamingOldVer1 {
	@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("WifiStreaming");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		// Create the context with 10 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(10000));
//		String zkQuorum = "147.47.206.15:32181";
		String zkQuorum = "localhost:2181";
		String group = "duclv";
		String topic = "wifi_tracking";
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, 1);
		JavaPairReceiverInputDStream<String, String> dStream = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);
		// get data
		JavaDStream lines = dStream.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				//encrypt mobile mac
				String row = tuple2._2();
				StringBuffer newRow = new StringBuffer();
				String[] items = row.split(",");
				newRow.append(items[0]).append(",")
					.append(items[1]).append(",")
					.append(items[2]).append(",")
					.append(items[3]).append(",")
					.append(items[4]).append(",")
					.append(encrypt(items[5])).append(",")
					.append(encrypt(items[6])).append(",")
					.append(encrypt(items[7])).append(",")
					.append(encrypt(items[8])).append(",")
					.append(encrypt(items[9])).append(",")
					.append(items[10]);
				return newRow.toString();
			}
		});
		//insert raw data
		lines.foreachRDD(new VoidFunction<JavaRDD>() {
			@Override
			public void call(JavaRDD rdd) {
				rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
					@Override
					public void call(Iterator<String> it) {
						Connection connection = null;
						try {
							//create connection to DB
							connection = DataSource.getInstance().getConnection();
							if(connection != null) {
								connection.setAutoCommit(false);
								//mobile data
								String sql = "insert into mobile_data("
										+ "sensor_id,time,channel,dBm,FCfield,addr1,addr2,addr3,addr4,mobile_mac,vendor) "
										+ " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
								PreparedStatement stmt = connection.prepareStatement(sql);
								DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								while(it.hasNext()) {
									String record = it.next();
									if(record.isEmpty()) {
										continue;
									}
									record = record.replaceAll("\'", "").replaceAll("\"", "");
									String[] items = record.split(",");
									int sensorId = parseInt(items[0]);
									Timestamp time = new Timestamp(format.parse(items[1]).getTime());
									
									stmt.setInt(1, sensorId);
									stmt.setTimestamp(2, time);
									stmt.setInt(3, parseInt(items[2]));
									stmt.setString(4, items[3]);
									stmt.setString(5, items[4]);
									stmt.setString(6, items[5]);
									stmt.setString(7, items[6]);
									stmt.setString(8, items[7]);
									stmt.setString(9, items[8]);
									stmt.setString(10, items[9]);
									stmt.setString(11, items[10]);
									stmt.addBatch();
								}
								//execute sql
								stmt.executeBatch();
								connection.commit();
								stmt.close();
							}
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							if(connection != null) {
								try {
									connection.close();
								} catch (SQLException e) {
									e.printStackTrace();
								}
							}
						}
					}
				});
			}
		});
		//create a map by key
		JavaPairDStream<String, String> pairLines = 
				lines.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				//create key based on time + macAddress + vendor
				String[] items = line.split(",");
				String key = items[1] + "," + items[9] + "," + items[10];
				key = Base64.encodeBase64String(key.getBytes());
				//value = sensorId + dBm
				String value = items[0] + "," + items[3];
				return new Tuple2<String, String>(key, value);
			}
		});
		//reduce by key
		//if line with the same sensorId then average the dBm
		//if different sensorId then add more columns
		JavaPairDStream<String, String> pairLinesReduced = 
				pairLines.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String line1, String line2) throws Exception {
				//split line
				String[] items = line1.split(",");
				String[] items2 = line2.split(",");
				//aggregate all items into result
				StringBuilder result = new StringBuilder();
				for(String item : items) {
					result = result.append(item).append(",");
				}
				for(String item2 : items2) {
					result = result.append(item2).append(",");
				}
				return result.toString();
			}
		});
		//get line data
		JavaDStream lineData = pairLinesReduced.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				//line has format: sensorId,dBm,sensorId,dBm,...
				String line = tuple2._2();
				String[] items = line.split(",");
				//process to average dbm of the same sensorId
				Map<String, Integer> mapByTotal = new HashMap<>();
				Map<String, Integer> mapByCount = new HashMap<>();
				for(int i = 0; i < items.length; ) {
					String sensorId = items[i];
					int dBm = parseInt(items[i+1]);
					Integer total = mapByTotal.get(sensorId);
					total = (total == null) ? dBm : total + dBm;
					mapByTotal.put(sensorId, total);
					Integer count = mapByCount.get(sensorId);
					count = (count == null) ? 1 : count + 1;
					mapByCount.put(sensorId, count);
					i+=2; //next to 2 items
				}
				//create aggregate column
				String aggColumn = "";
				Set<String> sensorList = mapByTotal.keySet();
				for(String sensorId : sensorList) {
					int total = mapByTotal.get(sensorId);
					int count = mapByCount.get(sensorId);
					int dbmAvg = total/count;
					aggColumn += sensorId + "_" + dbmAvg + ":";
				}
				if(aggColumn.lastIndexOf(":") == aggColumn.length() - 1) {
					aggColumn = aggColumn.substring(0, aggColumn.length() - 1);
				}
				String decodedKey = new String(Base64.decodeBase64(tuple2._1()));
				return decodedKey + "," + aggColumn;
			}
		});
		/*
		//load k-mean model
		final KMeansModel kMeanModel = KMeansModel.load(sc.sc(), 
				"file:///home/duclv/workspace/model/kMeanAvgDbm");
		//add prediction by k-mean
		lineData = lineData.map(new Function<String, String>() {
			@Override
			public String call(String record) throws Exception {
				//calculate maxDbm based on sensor1 & sensor5 fields
				java.lang.StringBuilder result = new java.lang.StringBuilder();
				String[] items = record.split(",");
				//get aggColumn
				String aggColumn = items[2];
				String[] aggItems = aggColumn.split(":");
				//find maxDbm
				int maxDbm = -100;
				for(int i = 0; i < aggItems.length; i++) {
					String aggItem = aggItems[i];
					String[] aggSubItem = aggItem.split("_");
					int aggDbm = parseInt(aggSubItem[1]);
					if(aggDbm > maxDbm) {
						maxDbm = aggDbm;
					}
				}
				//prediction
				int cluster = kMeanModel.predict(Vectors.dense(maxDbm*1.0d));
				cluster = (cluster == 2) ? 1 : 0;
				//create result
				result = result.append(items[0]).append(",")
								.append(items[1]).append(",")
								.append(items[2]).append(",")
								.append(cluster);
				return result.toString();
			}
		});*/
		//insert aggregate data and analysis data
		lineData.foreachRDD(new VoidFunction<JavaRDD>() {
			@Override
			public void call(JavaRDD rdd) {
				rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
					@Override
					public void call(Iterator<String> it) {
						Connection connection = null;
						try {
							//create connection to DB
							connection = DataSource.getInstance().getConnection();
							if(connection != null) {
								connection.setAutoCommit(false);
								//mobile data
								String sql = "insert into mobile_aggregate("
										+ "time,mobile_mac, vendor, "
										+ "sensor1,sensor2,sensor3,sensor4,sensor5,sensor6,sensor7) "
										+ "values(?, ?, ?, "
										+ "?, ?, ?, ?, ?, ?, ?)";
								//mobile location
								String sql2 = "insert into mobile_location(mobile_mac,time,location,dbm) "
										+ "values(?,?,?,?)";
								PreparedStatement stmt = connection.prepareStatement(sql);
								PreparedStatement stmt2 = connection.prepareStatement(sql2);
								DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								while(it.hasNext()) {
									String record = it.next();
									if(record.isEmpty()) {
										continue;
									}
									record = record.replaceAll("\'", "").replaceAll("\"", "");
									String[] items = record.split(",");
									Timestamp time = new Timestamp(format.parse(items[0]).getTime());
									String mobileMac = items[1];
									String vendor = items[2];
									int sensorId = 0;
									
									stmt.setTimestamp(1, time);
									stmt.setString(2, mobileMac);
									stmt.setString(3, vendor);
									stmt.setNull(4, Types.INTEGER);
									stmt.setNull(5, Types.INTEGER);
									stmt.setNull(6, Types.INTEGER);
									stmt.setNull(7, Types.INTEGER);
									stmt.setNull(8, Types.INTEGER);
									stmt.setNull(9, Types.INTEGER);
									stmt.setNull(10, Types.INTEGER);
									//get aggColumn
									String aggColumn = items[3];
									String[] aggItems = aggColumn.split(":");
									//find maxDbm
									int maxDbm = -100;
									for(int i = 0; i < aggItems.length; i++) {
										String aggItem = aggItems[i];
										String[] aggSubItem = aggItem.split("_");
										int aggSensorId = parseInt(aggSubItem[0]);
										int aggDbm = parseInt(aggSubItem[1]);
										if(aggDbm > maxDbm) {
											maxDbm = aggDbm;
											//update sensorId from aggSensorId
											sensorId = aggSensorId;
										}
										if(aggSensorId == 1) {
											stmt.setInt(4, aggDbm);
										}
										else if(aggSensorId == 2) {
											stmt.setInt(5, aggDbm);
										}
										else if(aggSensorId == 3) {
											stmt.setInt(6, aggDbm);
										}
										else if(aggSensorId == 4) {
											stmt.setInt(7, aggDbm);
										}
										else if(aggSensorId == 5) {
											stmt.setInt(8, aggDbm);
										}
										else if(aggSensorId == 6) {
											stmt.setInt(9, aggDbm);
										}
										else if(aggSensorId == 7) {
											stmt.setInt(10, aggDbm);
										}
									}
									stmt.addBatch();
									//set value for stmt2
									stmt2.setString(1, mobileMac);
									stmt2.setTimestamp(2, time);
									stmt2.setInt(3, sensorId);
									stmt2.setInt(4, maxDbm);
									stmt2.addBatch();
								}
								//execute sql
								stmt.executeBatch();
								stmt2.executeBatch();
								connection.commit();
								stmt.close();
								stmt2.close();
							}
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							if(connection != null) {
								try {
									connection.close();
								} catch (SQLException e) {
									e.printStackTrace();
								}
							}
						}
					}
				});
			}
		});
		// start
		jssc.start();
		jssc.awaitTermination();
		sc.close();
	}
	
	private static int parseInt(String s) {
		if(s == null || s.isEmpty()) {
			return 0;
		}
		try {
			return Integer.parseInt(s);
		}catch (NumberFormatException e) {
			return 0;
		}
	}
	
	private static String encrypt(String text) {
		try {
			MessageDigest md;
			md = MessageDigest.getInstance("SHA-256");
			md.update(text.getBytes());
			return bytesToHex(md.digest());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return text; //no encrypt
		}
	}
	public static String bytesToHex(byte[] bytes) {
        StringBuffer result = new StringBuffer();
        for (byte byt : bytes) result.append(Integer.toString((byt & 0xff) + 0x100, 16).substring(1));
        return result.toString().toUpperCase();
    }
}
