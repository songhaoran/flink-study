package com.song.flink.kafka.send;

import com.alibaba.fastjson.JSON;
import com.song.flink.kafka.bean.UserVisitEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.Resource;
import java.util.Random;
import java.util.UUID;

/**
 * Created by Song on 2019/12/25.
 */
@Component
@Slf4j
public class PvEventProducer {
    @Resource
    private KafkaTemplate kafkaTemplate;

    private String pvEventTopic = "pv_event";

    public void send(Integer size) {
        Random random = new Random();

        for (int i = 0; i < size; i++) {
            String yyyyMMdd = "20191225";
            int pageId = random.nextInt(10) + 1;    // 随机生成页面 id
            int userId = random.nextInt(100) + 1;   // 随机生成用户 id

            UserVisitEvent event = new UserVisitEvent();
            event.setId(UUID.randomUUID().toString());
            event.setDate(yyyyMMdd);
            event.setPageId(pageId);
            event.setUserId(Integer.toString(userId));
            event.setUrl("url/" + pageId);

            kafkaTemplate.send(pvEventTopic, JSON.toJSONString(event)).addCallback(new ListenableFutureCallback() {
                @Override
                public void onFailure(Throwable throwable) {
                    log.error("[send]send fail! page_id->{},user_id->{}", event.getPageId(), event.getUserId());
                }

                @Override
                public void onSuccess(Object o) {
                    log.info("[send]send success! page_id->{},user_id->{}", event.getPageId(), event.getUserId());
                }
            });
        }

        log.info("[send]send complete!");

    }
}
