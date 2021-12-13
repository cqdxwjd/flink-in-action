package yunli.httpdatasupplier.utils;

import yunli.httpdatasupplier.domain.GasMonitoringData;

import java.time.Instant;
import java.util.Random;

/**
 * @author wangjingdong
 * @date 2021/12/11 15:23
 * @Copyright © 云粒智慧 2018
 */
public class GasDataGenerator {
    private static final Instant beginTime = Instant.parse("2020-01-01T12:00:00.00Z");
    // 总共有多少家排污单位
    private static final int NUMBER_OF_UNITS = 10;
    // 每家排污单位有多少个传感器点位（排口）
    private static final int NUMBER_OF_SENSORS = 1;
    // 多长时间测量一次数据
    private static final int SECONDS_BETWEEN_MONITORING = 5;

    private final transient int unitId;
    private final transient int sensorId;
    // 表示每个传感器的第几条数据
    private final transient long dataId;

    /**
     * 为某个排口的某一条数据构造一个生成器对象
     */
    public GasDataGenerator(int unitId, int sensorId, long dataId) {
        this.unitId = unitId;
        this.sensorId = sensorId;
        this.dataId = dataId;
    }

    /**
     * Deterministically generates and returns the discUnitKey for this unit.
     */
    public String discUnitKey() {
        return "88888888000000" + unitId;
    }

    /**
     * Deterministically generates and returns the discUnitName for this unit.
     */
    public String discUnitName() {
        return "排污单位" + unitId;
    }

    /**
     * Deterministically generates and returns the outletKey for this outlet.
     */
    public String outletKey() {
        return discUnitKey() + "00" + sensorId;
    }

    /**
     * Deterministically generates and returns the outletName for this outlet.
     */
    public String outletName() {
        return "排污单位" + unitId + "的排口" + sensorId;
    }

    /**
     * Deterministically generates and returns the moniTime for this dataId.
     */
    public Instant moniTime() {
        return beginTime.plusSeconds(SECONDS_BETWEEN_MONITORING * dataId);
    }

    /**
     * Deterministically generates and returns the economicCode for this dataId.
     */
    public String economicCode() {
        return "D4411-火力发电";
    }

    /**
     * Deterministically generates and returns the faciType for this dataId.
     */
    public String faciType() {
        return "燃煤锅炉";
    }

    /**
     * Deterministically generates and returns the gasTemp for this dataId.
     */
    public double gasTemp() {
        return aFloat(70, 90);
    }

    /**
     * Deterministically generates and returns the gasPres for this dataId.
     */
    public double gasPres() {
        return 0;
    }

    /**
     * Deterministically generates and returns the gasSilentPres for this dataId.
     */
    public double gasSilentPres() {
        return 0;
    }

    /**
     * Deterministically generates and returns the atmos for this dataId.
     */
    public double atmos() {
        return 101325.0;
    }

    /**
     * Deterministically generates and returns the gasTunnelCsa for this dataId.
     */
    public double gasTunnelCsa() {
        return 0;
    }

    /**
     * Deterministically generates and returns the gasFlowRate for this dataId.
     */
    public double gasFlowRate() {
        return 0;
    }

    /**
     * Deterministically generates and returns the moisture for this dataId.
     */
    public double moisture() {
        return aFloat(7, 9);
    }

    /**
     * Deterministically generates and returns the moistO2 for this dataId.
     */
    public double moistO2() {
        return aFloat(5, 6);
    }

    /**
     * Deterministically generates and returns the realFlow for this dataId.
     */
    public double realFlow() {
        return aFloat(2, 3);
    }

    /**
     * Deterministically generates and returns the moistStanFlow for this dataId.
     */
    public double moistStanFlow() {
        return 0;
    }

    /**
     * Deterministically generates and returns the so2Real for this dataId.
     */
    public double so2Real() {
        return aFloat(20, 30);
    }

    /**
     * Deterministically generates and returns the noxReal for this dataId.
     */
    public double noxReal() {
        return aFloat(40, 45);
    }

    /**
     * Deterministically generates and returns the pmReal for this dataId.
     */
    public double pmReal() {
        return aFloat(1.5f, 1.7f);
    }

    private float aFloat(float min, float max) {
        float mean = (min + max) / 2.0F;
        float stddev = (max - min) / 8F;

        return aFloat(unitId * 100L + sensorId * 10L + dataId, min, max, mean, stddev);
    }

    // the seed is used as the seed to guarantee deterministic results
    private float aFloat(long seed, float min, float max, float mean, float stddev) {
        Random rnd = new Random(seed);
        float value;
        do {
            value = (float) (stddev * rnd.nextGaussian()) + mean;
        } while ((value < min) || (value > max));
        return value;
    }

    public static void main(String[] args) {
        GasMonitoringData data = new GasMonitoringData(1, 1, 2);
        System.out.println(data);
    }
}