package org.apache.flink.playgrounds.spendreport.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * @author wangjingdong
 * @date 2021/11/21 12:48
 * @Copyright © 云粒智慧 2018
 */
public class MyFloor extends ScalarFunction {
    public @DataTypeHint("TIMESTAMP(3)")
    LocalDateTime eval(
            @DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {
        return timestamp.truncatedTo(ChronoUnit.HOURS);
    }
}
