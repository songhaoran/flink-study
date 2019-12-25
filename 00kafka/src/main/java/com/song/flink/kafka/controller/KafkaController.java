package com.song.flink.kafka.controller;

import com.song.flink.kafka.send.PvEventProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * Created by Song on 2019/12/25.
 */
@RestController
public class KafkaController {

    @Resource
    private PvEventProducer pvEventProducer;

    @GetMapping(value = "/send/pv_event")
    public void sendPvEvent(@RequestParam(value = "size") Integer size) {
        pvEventProducer.send(size);
    }
}
