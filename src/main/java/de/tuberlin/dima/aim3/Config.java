package de.tuberlin.dima.aim3;

public class Config {
	
	public static String getTmpOutput() {
		return ClassLoader.getSystemClassLoader().getResource("").toString() + "../tmp/";
	}

}
