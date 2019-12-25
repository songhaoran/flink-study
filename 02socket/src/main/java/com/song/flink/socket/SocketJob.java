package com.song.flink.socket;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Created by Song on 2019/11/27.
 */
public class SocketJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("参数错误");
        }

        String host = args[0];
        Integer port = Integer.parseInt(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Object> sum = stream.flatMap((s, collector) -> {
            for (String token : s.toLowerCase().split("\\W+")) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        })
                .returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .keyBy(0)
                .sum(1);

        sum.print();

        env.execute("execute socket job!");
    }
}
