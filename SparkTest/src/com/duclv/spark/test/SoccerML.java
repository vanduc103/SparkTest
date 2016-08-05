package com.duclv.spark.test;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class SoccerML {
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SoccerML");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		//read file
		JavaRDD<String> rdd = sc.textFile("file:///home/duclv/Downloads/soccer/full-game");
		rdd.foreachPartition(new VoidFunction<Iterator<String>>() {

			@Override
			public void call(Iterator<String> it) throws Exception {
				// read into list
				List<BallPosition> lsBallPosition = new ArrayList<>();
				List<PlayerPosition> lsPlayerPosition = new ArrayList<>();
				Map<Integer, Integer> mapSidCount = new HashMap<>();
				int ballPosIndex = 0;
				while (it.hasNext()) {
					String line = it.next();
					// split line
					String[] items = line.split(",");
					int sid = Integer.parseInt(items[0]);
					long ts = Long.parseLong(items[1]);
					int x = Integer.parseInt(items[2]);
					int y = Integer.parseInt(items[3]);
					int z = Integer.parseInt(items[4]);
					
					// Ball
					if (sid == 4 || sid == 8 || sid == 10 || sid == 12) {
						//only get the ball inside field
						if (!(x < -1000 || x > 53000 || y > 34000 || y < -34000)) {
							BallPosition ball = new BallPosition();
							ball.setTs(ts);
							ball.setX(x);
							ball.setY(y);
							ball.setZ(z);
							lsBallPosition.add(ball);
							ballPosIndex++;
						}
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
							//player will keep next ball position index
							player.setBallPosIndex(ballPosIndex);
							lsPlayerPosition.add(player);
						}
						count++;
						mapSidCount.put(sid, count);
					}
				}
				//process data
				processData(lsBallPosition, lsPlayerPosition, 1);
				
				//save result
				String showFile = "/home/duclv/Downloads/soccer/ml/result_ml_"
									+System.currentTimeMillis()+".csv";
				try {
					File file2 = new File(showFile);
					file2.createNewFile();
					FileWriter writer = new FileWriter(file2);
					long exactStartTime = 10753295594424116l;
					for(PlayerPosition player : lsPlayerPosition) {
						int hasBall = player.getHasBall();
						if(hasBall == 1) {
							long ts = player.getTs();
							long timeDistance = ts - exactStartTime;
							//convert time distance to seconds (plus 5 seconds)
							timeDistance = timeDistance / 1000000000000l + 5;
							StringBuilder result = new StringBuilder();
							result.append(player.getSid()).append(",")
									.append(timeDistance).append(",")
									.append(player.getX()).append(",")
									.append(player.getY()).append(",")
									.append(player.getZ()).append("\n");
							writer.write(result.toString());
						}
					}
					writer.flush();
					writer.close();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		});
		sc.close();
	}
	
	private static void processData(
			List<BallPosition> lsBallPosition, 
			List<PlayerPosition> lsPlayerPosition,
			int playerPosIndex) {
		/**
		 * With each player position: check within 1 second. Between this
		 * time period, if it has ball close to the player in 0.5m then add
		 * 1 and the coordinate of the ball else add 0
		 */
		int ballPosCount = lsBallPosition.size();
		int playerPosCount = lsPlayerPosition.size();
		for(int k = playerPosIndex; k < playerPosCount; k++) {
			//get k from mapSidIndex
			PlayerPosition player = lsPlayerPosition.get(k);
			int playerX = player.getX();
			int playerY = player.getY();
			int startIndex = player.getBallPosIndex();
			for(int i = startIndex; i < ballPosCount && i < (startIndex + 2000); i++) {
				BallPosition ballPos = lsBallPosition.get(i);
				int ballX = ballPos.getX();
				int ballY = ballPos.getY();
				int ballZ = ballPos.getZ();
				//if distance in x or y > 0.5m or z >= 2m then continue
				if(ballZ > 2000 || 
						Math.abs(ballX-playerX) > 0.5*1000 || Math.abs(ballY-playerY) > 0.5*1000) {
					continue;
				}
				double ballPlayerDistance = Math.hypot((ballX-playerX), (ballY-playerY));
				if(ballPlayerDistance <= 0.5*1000) {
					player.setHasBall(1);
					player.setBallX(ballX);
					player.setBallY(ballY);
					player.setBallZ(ballZ);
					break;
				}
			}
		}
	}
}
