package de.tuberlin.dima.aim3;

public class Config {

    public static String getTmpOutput() {
        return ClassLoader.getSystemClassLoader().getResource("").toString() + "../tmp/";
    }

    public static String getOutputPath() {
        return "results/";
    }

    public static String pathToTmm() {
        return getOutputPath() + "Tmm.out";
    }

    public static String pathToVm() {
        return getOutputPath() + "Vm.out";
    }
}
