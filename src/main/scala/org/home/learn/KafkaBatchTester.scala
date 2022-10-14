package org.home.learn

import org.apache.flink.api.common.ExecutionMode
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.eventtime.WatermarkStrategy.forMonotonousTimestamps
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.home.learn.models.{StringPartitioner, StringProcessFunction, TestString, TestStringEncoder, TestStringSchema, TestStringWindowFunction}

import java.net.URI
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

object KafkaBatchTester {
  val CHECKPOINT_INTERVAL = 60000L
  val MAX_CONCURRENT_CHECKPOINTS = 1
  val CHECKPOINT_FAILURE_TOLERANCE = 1

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.enableCheckpointing(CHECKPOINT_INTERVAL)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECKPOINTS)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(CheckpointConfig.DEFAULT_TIMEOUT)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(CHECKPOINT_FAILURE_TOLERANCE)

    val config = env.getConfig
//    config.setExecutionMode(ExecutionMode.BATCH)

//    val sink: StreamingFileSink[TestString] = StreamingFileSink
//      .forRowFormat(new Path(URI.create("/Users/weiwang/temp/flink/kafka")), new TestStringEncoder())
//      .withRollingPolicy(
//        DefaultRollingPolicy.builder()
//          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//          .withMaxPartSize(1024 * 1024 * 1024)
//          .build())
//      .build()

    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("auto.offset.reset", "latest")

    val kafkaConsumer = new FlinkKafkaConsumer("test-topic", new TestStringSchema(), props)

//    env.addSource(kafkaConsumer)
//      .assignTimestampsAndWatermarks(
//        WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner[TestString] {
//          override def extractTimestamp(element: TestString, recordTimestamp: Long): Long = recordTimestamp
//        }))
//      .keyBy(_.key)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//      .reduce((r1, r2) => if(r1.timeStamp > r2.timeStamp) r1 else r2)
//      .print()

    val source: KafkaSource[String] = KafkaSource.builder()
      .setBootstrapServers("localhost:9092")
      .setTopics("test-topic")
      .setGroupId("test-group")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build()

    env
      .fromSource(source, WatermarkStrategy.forMonotonousTimestamps[String]().withTimestampAssigner(
        new SerializableTimestampAssigner[String] {
          override def extractTimestamp(t: String, l: Long): Long = l
        }
      ), "Kafka Source")
//      .assignTimestampsAndWatermarks(
//        WatermarkStrategy.forMonotonousTimestamps[TestString]()
//          .withTimestampAssigner(new SerializableTimestampAssigner[TestString] {
//            override def extractTimestamp(element: TestString, recordTimestamp: Long): Long = element.timeStamp
//          }))
//      .keyBy(_.toInt)
//      .partitionCustom(new StringPartitioner, _.toString)
      .keyBy(_.toInt % 2)
//      .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
      .process(new StringProcessFunction)
//      .process(new TestStringWindowFunction)
//      .print()
      .print()

//    env.fromElements(
//        TestString("a",1657529849686L),
//        TestString("b",1657529849685L),
//        TestString("a",1657529849684L),
//        TestString("b",1657529849683L),
//        TestString("b",1657529849682L),
//        TestString("a",1657529849681L),
//        TestString("a",1657529849688L),
//        TestString("a",1657529849689L)
//    )
//      .assignTimestampsAndWatermarks(
//        WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner[TestString] {
//          override def extractTimestamp(element: TestString, recordTimestamp: Long): Long = element.timeStamp
//        }))
//      .keyBy(_.key)
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .process(new TestStringWindowFunction)
//      .print()

    env.execute()
  }
}
