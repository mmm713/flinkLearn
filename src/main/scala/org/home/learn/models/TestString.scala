package org.home.learn.models

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.serialization.{DeserializationSchema, Encoder, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.io.OutputStream

case class TestString
(
  @JsonProperty("key") key: String,
  @JsonProperty("timeStamp") timeStamp: Long
)

class TestStringEncoder extends Encoder[TestString]{
  @transient lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def encode(in: TestString, outputStream: OutputStream): Unit = {
    outputStream.write(mapper.writeValueAsBytes(in))
  }
}

class TestStringWindowFunction extends ProcessWindowFunction[TestString, TestString, String, TimeWindow] {

  def process(key: String, context: Context, input: Iterable[TestString], out: Collector[TestString]): Unit = {
    var res: TestString = TestString("test", 1657328540000L)
    for (in <- input) {
      if(in.timeStamp > res.timeStamp) {
        res = in
      }
    }
    out.collect(res)
  }
}

class StringPartitioner extends Partitioner[String] {
  override def partition(k: String, i: Int): Int =
    k.toInt % i
}

class StringProcessAllFunction extends ProcessAllWindowFunction[String, String, TimeWindow] {
  def process(context: Context, input: Iterable[String], out: Collector[String]): Unit = {
    var res: String = ""
    for (in <- input) {
      res += s"_$in"
    }
    out.collect(res)
  }
}

class StringProcessFunction extends ProcessWindowFunction[String, String, Int, TimeWindow] {

  def process(key: Int, context: Context, input: Iterable[String], out: Collector[String]): Unit = {
    var res: String = ""
    for (in <- input) {
      res += s"_$in"
    }
    res += s":$key:${getRuntimeContext.getIndexOfThisSubtask}:${getRuntimeContext.getNumberOfParallelSubtasks}"
    out.collect(res)
  }
}

class TestStringSchema extends DeserializationSchema[TestString] with SerializationSchema[TestString]{
  @transient lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def deserialize(bytes: Array[Byte]): TestString = mapper.readValue(bytes, classOf[TestString])

  override def isEndOfStream(t: TestString): Boolean = false

  override def getProducedType: TypeInformation[TestString] = TypeInformation.of(classOf[TestString])

  override def serialize(t: TestString): Array[Byte] = mapper.writeValueAsBytes(t)
}