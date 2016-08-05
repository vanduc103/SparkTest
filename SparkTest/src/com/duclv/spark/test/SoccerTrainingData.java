package com.duclv.spark.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SoccerTrainingData {
	public static void main(String[] args) {
		String dataFile = "/home/duclv/Downloads/soccer/referee-events/Ball Possession/1st Half/Christopher Lee.csv";
		Map<Integer, List<Long>> mapTimeEvent = new HashMap<>();
		long matchStartTime = 10753295594424116l;
		File file = new File(dataFile);
		System.out.println("1: " + System.currentTimeMillis());
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = null;
			while((line = reader.readLine()) != null) {
				if(line.startsWith("6014") || line.startsWith("6015")) {
					String[] items = line.split(";");
					String[] time = items[2].split(":");
					int hour = Integer.parseInt(time[0]);
					int minute = Integer.parseInt(time[1]);
					int second = Integer.parseInt(time[2].split("\\.")[0]);
					int milisecond = Integer.parseInt(time[2].split("\\.")[1]);
					long possessionTime = matchStartTime 
							+ (long) ((hour*3600 + minute*60 + second)*1000 + milisecond)*1000000000;
					List<Long> lsTimeEvent = mapTimeEvent.get(71);
					if(lsTimeEvent == null) {
						lsTimeEvent = new ArrayList<>();
					}
					lsTimeEvent.add(possessionTime);
					//put time event for this player
					mapTimeEvent.put(71, lsTimeEvent);
					mapTimeEvent.put(40, lsTimeEvent);
				}
			}
			reader.close();
			String sourceFile = "/home/duclv/Downloads/soccer/test.csv";
			String trainingFilePath = "/home/duclv/Downloads/soccer/test_training.csv";

			// read into list
			List<BallPosition> lsBallPosition = new ArrayList<>();
			lsBallPosition.add(new BallPosition()); //add first not null element
			List<PlayerPosition> lsPlayerPosition = new ArrayList<>();
			try {
				File file2 = new File(sourceFile);
				BufferedReader reader2 = new BufferedReader(new FileReader(file2));
				String line2 = null;
				int ballPosIndex = 0;
				System.out.println("2: " + System.currentTimeMillis());
				Map<Integer, Integer> mapSidCount = new HashMap<>();
				while ((line2 = reader2.readLine()) != null) {
					// split line
					String[] items = line2.split(",");
					int sid = Integer.parseInt(items[0]);
					long ts = Long.parseLong(items[1]);
					int x = Integer.parseInt(items[2]);
					int y = Integer.parseInt(items[3]);
					int z = Integer.parseInt(items[4]);
					long v = Long.parseLong(items[5]);
					long a = Long.parseLong(items[6]);
					
					// Ball
					if (sid == 4 || sid == 8 || sid == 10 || sid == 12) {
						//count times of this ball
						//if < 100 (1/20 of 2000Hz) then do not add to list
						int count = mapSidCount.get(sid) == null ? 0 : mapSidCount.get(sid);
						if(count == 100) {
							//reset to add sid to list
							count = 0;
						}
						//only get the ball inside field
						if (count == 0 && !(x < -1000 || x > 53000 || y > 34000 || y < -34000)) {
							BallPosition ball = new BallPosition();
							ball.setTs(ts);
							ball.setX(x);
							ball.setY(y);
							ball.setZ(z);
							ball.setV(v);
							ball.setA(a);
							lsBallPosition.add(ball);
							ballPosIndex++;
						}
						count++;
						mapSidCount.put(sid, count);
					}
					// player, not referee
					else if (sid != 105 && sid != 106) {
						//count times of this sid
						//if < 50 (1/4 of 200Hz) then do not add to list
						int count = mapSidCount.get(sid) == null ? 0 : mapSidCount.get(sid);
						if(count == 50) {
							//reset to add sid to list
							count = 0;
						}
						if(count == 0) {
							PlayerPosition player = new PlayerPosition();
							player.setSid(sid);
							player.setTs(ts);
							player.setX(x);
							player.setY(y);
							player.setZ(z);
							//player will keep next ball information
							BallPosition ball = lsBallPosition.get(ballPosIndex);
							player.setBallX(ball.getX());
							player.setBallY(ball.getY());
							player.setBallZ(ball.getZ());
							player.setBallV(ball.getV());
							player.setBallA(ball.getA());
							lsPlayerPosition.add(player);
						}
						count++;
						mapSidCount.put(sid, count);
					}
				}
				reader2.close();
				System.out.println("3: "+System.currentTimeMillis());
				//write to training file
				File trainingFile = new File(trainingFilePath);
				trainingFile.createNewFile();
				PrintWriter trainingWriter = new PrintWriter(new FileWriter(trainingFile));
				for(PlayerPosition player : lsPlayerPosition) {
					//find player who keeps the ball
					int sid = player.getSid();
					long ts = player.getTs();
					List<Long> lsTimeEvent = mapTimeEvent.get(sid);
					int hasBall = 0;
					//check by each pair of list time event
					if(lsTimeEvent != null) {
						int size = lsTimeEvent.size();
						for(int i = 0; i < size; ) {
							if(ts > lsTimeEvent.get(i) && ts < lsTimeEvent.get(i+1)) {
								hasBall = 1;
								break;
							}
							i += 2; //jump to next pair
						}
					}
					player.setHasBall(hasBall);
					StringBuilder out = new StringBuilder();
					out.append(player.getHasBall()).append(" ")
						.append("1:").append(player.getX()).append(" ")
						.append("2:").append(player.getY()).append(" ")
						.append("3:").append(player.getZ()).append(" ")
						.append("4:").append(player.getBallX()).append(" ")
						.append("5:").append(player.getBallY()).append(" ")
						.append("6:").append(player.getBallZ()).append(" ")
						.append("7:").append(player.getBallV()).append(" ")
						.append("8:").append(player.getBallA()).append("\n");
					trainingWriter.write(out.toString());
				}
				trainingWriter.flush();
				trainingWriter.close();
				System.out.println("4: "+System.currentTimeMillis());
				
			}catch(Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
