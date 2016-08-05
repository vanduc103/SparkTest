package com.duclv.spark.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BrmhTrainingData {
	public static void main(String[] args) {
		Connection conn = null;
		try {
			System.out.println("1: " + new Date());
			Calendar cal = Calendar.getInstance();
			//cal = setDate(cal, 10, 4, 2016); //10 May 2016
			long endTime = cal.getTimeInMillis(); //current day
			cal = setDate(cal, 1, 7, 2016); //01 08 2016
			//long startTime = endTime - 10*24*3600*1000; //10 days ago
			long startTime = cal.getTimeInMillis();
			Map<String, BrmhKmeanData> mapKmeanData = new HashMap<>();
			String sectionFilePath = "/home/duclv/workspace/brmh_real_seq_section.txt";
			File sectionFile = new File(sectionFilePath);
			sectionFile.createNewFile();
			PrintWriter sectionWriter = new PrintWriter(sectionFile);
			
			conn = DataSource.getInstance().getConnection();
			String sql = "SELECT sensor_id, time, mobile_mac, dBm from mobile_data "
						+ "where 1=1 and time >= ? "
						+ "order by mobile_mac, time";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, new Timestamp(startTime));
			ResultSet rs = stmt.executeQuery();
			Map<Integer, Integer> mapSensorCount = new HashMap<>();
			long preTime = 0l;
			while(rs.next()) {
				int sensorId = rs.getInt(1);
				long time = rs.getTimestamp(2).getTime();
				String mobileMac = rs.getString(3);
				int dbm = rs.getInt(4);
				
				BrmhKmeanData kmeanData = mapKmeanData.get(mobileMac);
				if(kmeanData == null) {
					kmeanData = new BrmhKmeanData();
				}
				// keep pre time
				if(preTime == 0l) {
					preTime = time;
				}
				mapSensorCount.put(sensorId, 1);
				// seen next time
				if(time != preTime) {
					//calculate number of different sensors
					int numOfSensor = mapSensorCount.keySet().size();
					mapSensorCount.clear(); // reset
					time = preTime;
					kmeanData.setNumOfSensorTotal(kmeanData.getNumOfSensorTotal() + numOfSensor);
					kmeanData.setNumOfSensorCount(kmeanData.getNumOfSensorCount() + 1);
					if(numOfSensor > 1) {
						System.out.println("NumOfSensor: " + numOfSensor);
						System.out.println("Total: " + kmeanData.getNumOfSensorTotal());
						System.out.println("Count: " + kmeanData.getNumOfSensorCount());
					}
				}
				kmeanData.setDbmTotal(kmeanData.getDbmTotal() + dbm);
				kmeanData.setCount(kmeanData.getCount() + 1);
				//check stored hour
				int storedHour = (int) (time - startTime) / (3600*1000);
				if(kmeanData.getStoredHour() != storedHour) {
					kmeanData.setStoredHour(storedHour);
					kmeanData.setNumberOfHours(kmeanData.getNumberOfHours() + 1);
				}
				mapKmeanData.put(mobileMac, kmeanData);
			}
			rs.close();
			stmt.close();
			//print to brmh section file
			/*Set<String> macSet = mapSensors.keySet();
			for(String mac : macSet) {
				sectionWriter.println(mac);
				List<Integer> lsSensor = mapSensors.get(mac);
				int size = lsSensor.size();
				for(int i = 0; i + 3 < size; i++) {
					sectionWriter.println(lsSensor.get(i)+" "
							+lsSensor.get(i + 1)+" "+lsSensor.get(i+2)
							+" " +lsSensor.get(i+3));
				}
			}*/
			sectionWriter.flush();
			sectionWriter.close();
			System.out.println("2: " + new Date());
			//process result and save to training file
			String outFilePath = "/home/duclv/workspace/brmh_real_training.txt";
			File file = new File(outFilePath);
			file.createNewFile();
			PrintWriter writer = new PrintWriter(new FileWriter(file));
			writer.println("Mac address,dBm average,number of hours,average appearence per hour,average number of sensors");
			Set<String> keySet = mapKmeanData.keySet();
			for(String key : keySet) {
				BrmhKmeanData kmeanData = mapKmeanData.get(key);
				if(kmeanData.getNumberOfHours() > 0) {
					kmeanData.setAvgAppearPerHour(kmeanData.getCount() / kmeanData.getNumberOfHours());
				}
				kmeanData.setAvgDbm(kmeanData.getDbmTotal() / kmeanData.getCount());
				if(kmeanData.getNumOfSensorCount() > 0) {
					kmeanData.setAvgNumOfSensor(kmeanData.getNumOfSensorTotal() / kmeanData.getNumOfSensorCount());
				}
				StringBuilder out = new StringBuilder();
				out.append(key).append(",").append(kmeanData.getAvgDbm()).append(",")
					.append(kmeanData.getNumberOfHours()).append(",")
					.append(kmeanData.getAvgAppearPerHour()).append(",")
					.append(kmeanData.getAvgNumOfSensor())
					.append("\n");
				writer.write(out.toString());
			}
			writer.flush();
			writer.close();
			System.out.println("3: " + new Date());
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static Calendar setTime(Calendar oldTime, int hour, int minute, int second) {
		oldTime.set(oldTime.get(
				Calendar.YEAR), oldTime.get(Calendar.MONTH), oldTime.get(Calendar.DATE), 
				hour, minute, second);
		oldTime.set(Calendar.MILLISECOND, 0);
		return oldTime;
	}
	
	private static Calendar setDate(Calendar oldTime, int date, int month, int year) {
		oldTime.set(year, month, date, 0, 0, 0);
		oldTime.set(Calendar.MILLISECOND, 0);
		return oldTime;
	}
}
