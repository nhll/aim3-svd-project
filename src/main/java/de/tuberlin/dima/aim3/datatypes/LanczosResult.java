package de.tuberlin.dima.aim3.datatypes;

import org.apache.flink.api.java.DataSet;

public class LanczosResult {

    private DataSet<Vector> Tmm;
    private DataSet<Vector> Vm;

    public LanczosResult(DataSet<Vector> Tmm, DataSet<Vector> Vm) {
        this.Tmm = Tmm;
        this.Vm = Vm;
    }

    public DataSet<Vector> getTmm() {
        return Tmm;
    }

    public DataSet<Vector> getVm() {
        return Vm;
    }
}
