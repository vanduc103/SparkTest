package com.duclv.spark.test;

public class PlayerPosition {
	private int sid;
	private long ts;
	private int x;
	private int y;
	private int z;
	private int hasBall = 0;
	private int ballX = 0;
	private int ballY = 0;
	private int ballZ = 0;
	private long ballV = 0;
	private long ballA = 0;
	private int ballPosIndex = 0;
	
	public int getSid() {
		return sid;
	}
	public void setSid(int sid) {
		this.sid = sid;
	}
	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}
	public int getX() {
		return x;
	}
	public void setX(int x) {
		this.x = x;
	}
	public int getY() {
		return y;
	}
	public void setY(int y) {
		this.y = y;
	}
	public int getZ() {
		return z;
	}
	public void setZ(int z) {
		this.z = z;
	}
	public int getBallPosIndex() {
		return ballPosIndex;
	}
	public void setBallPosIndex(int ballPosIndex) {
		this.ballPosIndex = ballPosIndex;
	}
	public int getHasBall() {
		return hasBall;
	}
	public void setHasBall(int hasBall) {
		this.hasBall = hasBall;
	}
	public int getBallX() {
		return ballX;
	}
	public void setBallX(int ballX) {
		this.ballX = ballX;
	}
	public int getBallY() {
		return ballY;
	}
	public void setBallY(int ballY) {
		this.ballY = ballY;
	}
	public int getBallZ() {
		return ballZ;
	}
	public void setBallZ(int ballZ) {
		this.ballZ = ballZ;
	}
	public long getBallV() {
		return ballV;
	}
	public void setBallV(long ballV) {
		this.ballV = ballV;
	}
	public long getBallA() {
		return ballA;
	}
	public void setBallA(long ballA) {
		this.ballA = ballA;
	}
	
}
