package de.tuberlin.dima.aim3;

public class Config {
	
	public static String getTmpOutput() {
		return ClassLoader.getSystemClassLoader().getResource("").toString() + "../tmp/";
	}

    public static final byte idOfCorpus = 0;

    public static final byte idOfBasis = 1;

    public static final byte idOfTriag = 2;

}
