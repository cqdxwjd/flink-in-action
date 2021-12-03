package org.apache.flink.training.exercises.common.datatypes;

import org.apache.flink.training.exercises.common.utils.GeoUtils;

/**
 * @author wangjingdong
 * @date 2021/11/25 16:27
 * @Copyright © 云粒智慧 2018
 */
public class EnrichedRide extends TaxiRide {
    public int startCell;
    public int endCell;

    public EnrichedRide() {
    }

    public EnrichedRide(TaxiRide ride) {
        this.rideId = ride.rideId;
        this.isStart = ride.isStart;
        this.startTime = ride.startTime;
        this.endTime = ride.endTime;
        this.startLon = ride.startLon;
        this.startLat = ride.startLat;
        this.endLon = ride.endLon;
        this.endLat = ride.endLat;
        this.passengerCnt = ride.passengerCnt;
        this.taxiId = ride.taxiId;
        this.driverId = ride.driverId;
        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
        this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
    }

    @Override
    public String toString() {
        return "EnrichedRide{" +
                "startCell=" + startCell +
                ", endCell=" + endCell +
                ", rideId=" + rideId +
                ", isStart=" + isStart +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", startLon=" + startLon +
                ", startLat=" + startLat +
                ", endLon=" + endLon +
                ", endLat=" + endLat +
                ", passengerCnt=" + passengerCnt +
                ", taxiId=" + taxiId +
                ", driverId=" + driverId +
                '}';
    }
}
