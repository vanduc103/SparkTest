package com.duclv.spark.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BrmhUpdateCluster {

	public static void main(String[] args) {
		String filePath = "/home/duclv/workspace/brmh_update_cluster";
		Connection conn = null;
		try {
			//connect to HanaDB
			conn = DataSource.getInstance().getConnection();
			String sql = "UPDATE WIFI_BACKUP.MOBILE_MAC SET cluster = 0 WHERE cluster IS NULL "
					+ "AND MOBILE_MAC = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			conn.setAutoCommit(false);
			int batchSize = 100;
			int i = 0;
			//read sql command from file
			File file = new File(filePath);
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				stmt.setString(1, line);
				stmt.addBatch();
				i++;
				if(i % batchSize == 0) {
					stmt.executeBatch();
					conn.commit();
				}
			}
			reader.close();
			stmt.executeBatch();
			conn.commit();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
