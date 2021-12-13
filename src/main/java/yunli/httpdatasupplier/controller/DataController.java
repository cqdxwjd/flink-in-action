package yunli.httpdatasupplier.controller;

import com.google.gson.Gson;
import org.springframework.web.bind.annotation.*;
import yunli.httpdatasupplier.domain.GasMonitoringData;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author wangjingdong
 * @date 2021/12/10 16:14
 * @Copyright © 云粒智慧 2018
 */
@RestController
public class DataController {
    private static final Gson gson = new Gson();
    private int batch;
    private static final int NUMBER_OF_BATCH = 10000;

    @PostConstruct
    private void init() throws InterruptedException {
        batch = 0;
    }

    @RequestMapping(value = "/gas", method = RequestMethod.GET)
    @ResponseBody
    public String gasDataReport() throws IOException, InterruptedException {
        ArrayList<GasMonitoringData> list = new ArrayList<>();
        for (int i = batch * NUMBER_OF_BATCH; i < (batch + 1) * NUMBER_OF_BATCH; i++) {
            list.add(new GasMonitoringData(1, 1, i));
        }
        batch++;
        return gson.toJson(list);
    }
}