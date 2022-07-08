package org.home.learn

import org.apache.flink.api.common.ExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object KafkaBatchTester {
  val CHECKPOINT_INTERVAL = 60000L
  val MAX_CONCURRENT_CHECKPOINTS = 1
  val CHECKPOINT_FAILURE_TOLERANCE = 1

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(CHECKPOINT_INTERVAL)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECKPOINTS)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(CheckpointConfig.DEFAULT_TIMEOUT)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(CHECKPOINT_FAILURE_TOLERANCE)

    val config = env.getConfig
    config.setExecutionMode(ExecutionMode.BATCH)
    val source: KafkaSource[String] = KafkaSource.builder()
      .setBootstrapServers("localhost:9092")
      .setTopics("test-topic")
      .setGroupId("test-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    env
      .fromSource(source,
        WatermarkStrategy.noWatermarks[String],
        "Kafka Source")
      .print()
    env.execute()
  }
}
