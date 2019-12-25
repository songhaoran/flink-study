package com.song.flink.pv;

import com.alibaba.fastjson.JSON;
import com.song.flink.pv.bean.UserVisitEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Created by Song on 2019/12/25.
 *
 */
@Slf4j
public class PvJob {

    private static String pvEventTopic = "pv_event";

    public static void main(String[] args) throws Exception {

        //  省略了 env初始化及 checkpoint 相关配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty("zookeeper.connect", "localhost:2181");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "gid_pv_event");

        FlinkKafkaConsumerBase<String> kafkaConsumer = new FlinkKafkaConsumer011<>(
                pvEventTopic, new SimpleStringSchema(), props)
                .setStartFromLatest();

        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig
                .Builder().setHost("127.0.0.1").build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
        env.setParallelism(2);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.addSource(kafkaConsumer)
                .map(new MapFunction<String, UserVisitEvent>() {
                    @Override
                    public UserVisitEvent map(String s) throws Exception {
                        System.out.println(s);
                        return JSON.parseObject(s, UserVisitEvent.class);
                    }
                })
                .keyBy(new KeySelector<UserVisitEvent, Object>() {
                    @Override
                    public Object getKey(UserVisitEvent userVisitEvent) throws Exception {
                        return userVisitEvent.getPageId();
                    }
                })
                .map(new RichMapFunction<UserVisitEvent, Tuple2<String, Long>>() {

                    // 存储当前 key 对应的 userId 集合
                    private MapState<String, Boolean> userIdState;
                    // 存储当前 key 对应的 UV 值
                    private ValueState<Long> uvState;

                    @Override
                    public Tuple2<String, Long> map(UserVisitEvent userVisitEvent) throws Exception {
                        if (uvState.value() == null) {
                            uvState.update(0l);
                        }

                        if (userVisitEvent == null) {
                            return null;
                        }
                        String userId = userVisitEvent.getUserId();
                        if (StringUtils.isBlank(userId)) {
                            return null;
                        }

                        if (userIdState.contains(userId) == false) {
                            userIdState.put(userId, true);
                            uvState.update(uvState.value() + 1);
                        }

                        String key = userVisitEvent.getDate() + "_" + userVisitEvent.getPageId();

                        log.info("[]key->{},value->{}", key, uvState.value());
                        return Tuple2.of(key, uvState.value());
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        userIdState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("userIdState",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Boolean>() {
                                        })));

                        uvState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("uvState",
                                TypeInformation.of(new TypeHint<Long>() {
                                })));
                    }


                })
                .addSink(new RedisSink<>(redisConf, new RedisMapper<Tuple2<String, Long>>() {
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.SET);
                    }

                    @Override
                    public String getKeyFromData(Tuple2<String, Long> stringLongTuple2) {
                        log.info("[]key->{}", stringLongTuple2.f0);
                        return stringLongTuple2.f0;
                    }

                    @Override
                    public String getValueFromData(Tuple2<String, Long> stringLongTuple2) {
                        log.info("[]value->{}", stringLongTuple2.f1);
                        return stringLongTuple2.f1.toString();
                    }
                }));

        env.execute("uvJob");
    }
}
