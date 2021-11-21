/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playgrounds.spendreport;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.playgrounds.spendreport.udf.MyFloor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class SpendReport {

    public static Table report(Table transactions) {
        throw new UnimplementedException();
    }

    public static Table newReport(Table transactions) {
//        return transactions.select(
//                $("account_id"),
//                $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
//                $("amount"))
//                .groupBy($("account_id"), $("log_ts"))
//                .select(
//                        $("account_id"),
//                        $("log_ts"),
//                        $("amount").sum().as("amount"));

        // 使用自定义函数
//        return transactions.select(
//                $("account_id"),
//                call(MyFloor.class, $("transaction_time")).as("log_ts"),
//                $("amount"))
//                .groupBy($("account_id"), $("log_ts"))
//                .select(
//                        $("account_id"),
//                        $("log_ts"),
//                        $("amount").sum().as("amount"));

        // 使用窗口
        return transactions
                .window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("log_ts"))
                .groupBy($("account_id"), $("log_ts"))
                .select(
                        $("account_id"),
                        $("log_ts").start().as("log_ts"),
                        $("amount").sum().as("amount"));
    }

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        Configuration configuration = tEnv.getConfig().getConfiguration();
        // 设置默认并行度为2
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 3);
        // 设置source超时空闲时间，在1.12.2修复配置不生效的问题，之前的版本中即使设置了也不会生效，参考 FLINK-20947
        configuration.set(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT, Duration.ofSeconds(1));

        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'heifen-227:9092,heifen-229:9092,heifen-231:9092',\n" +
                "    'format'    = 'csv'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE spend_report (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP(3),\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector'  = 'jdbc',\n" +
                "  'url'        = 'jdbc:mysql://heifen-231:3306/daily_test',\n" +
                "  'table-name' = 'spend_report',\n" +
                "  'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "  'username'   = 'root',\n" +
                "  'password'   = '1qazxsw@'\n" +
                ")");

        Table transactions = tEnv.from("transactions");
        newReport(transactions).executeInsert("spend_report");
    }
}
