package yunli.httpdatasupplier.domain;

import lombok.Data;
import yunli.httpdatasupplier.utils.GasDataGenerator;

import java.time.Instant;

/**
 * 废气监测数据
 *
 * @author wangjingdong
 * @date 2021/12/10 17:16
 * @Copyright © 云粒智慧 2018
 */
@Data
public class GasMonitoringData {
    String discUnitKey;
    String discUnitName;
    String outletKey;
    String outletName;
    Instant moniTime;
    String economicCode;
    String faciType;
    double gasTemp;
    double gasPres;
    double gasSilentPres;
    double atmos;
    double gasTunnelCsa;
    double gasFlowRate;
    double moisture;
    double moistO2;
    double realFlow;
    double moistStanFlow;
    double so2Real;
    double noxReal;
    double pmReal;

    public GasMonitoringData(int unitId, int sensorId, long dataId) {
        GasDataGenerator g = new GasDataGenerator(unitId, sensorId, dataId);
        this.discUnitKey = g.discUnitKey();
        this.discUnitName = g.discUnitName();
        this.outletKey = g.outletKey();
        this.outletName = g.outletName();
        this.moniTime = g.moniTime();
        this.economicCode = g.economicCode();
        this.faciType = g.faciType();
        this.gasTemp = g.gasTemp();
        this.gasPres = g.gasPres();
        this.gasSilentPres = g.gasSilentPres();
        this.atmos = g.atmos();
        this.gasTunnelCsa = g.gasTunnelCsa();
        this.gasFlowRate = g.gasFlowRate();
        this.moisture = g.moisture();
        this.moistO2 = g.moistO2();
        this.realFlow = g.realFlow();
        this.moistStanFlow = g.moistStanFlow();
        this.so2Real = g.so2Real();
        this.noxReal = g.noxReal();
        this.pmReal = g.pmReal();
    }
}
