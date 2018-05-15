package edu.knoldus.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * Simple example on how to read with a Kafka consumer
 *   --topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myGroup
 */
public class ReadFromKafkaWithCheckpoint {

	public static void main(String[] args) throws Exception {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);

		// parse user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));

		// print() will write the contents of the stream to the TaskManager's standard out stream
		// the re-balance call is causing a repartitioning of the data so that all machines
		// see the messages (for example in cases when "num kafka partitions" < "num flink operators"
		messageStream.rebalance().map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -686773677174769022L;

			@Override
			public String map(String value) {
				return "Kafka and Flink says: " + value;
			}
		}).print();

		env.execute();
	}
}
