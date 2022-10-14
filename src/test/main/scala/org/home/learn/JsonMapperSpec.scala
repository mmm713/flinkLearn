package org.home.learn

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.home.learn.models.TestString
import org.scalatest.flatspec.AnyFlatSpec

class JsonMapperSpec extends AnyFlatSpec {
  "A JsonMapper" should "read and write json values" in {
    val t = TestString("test", System.currentTimeMillis())

    val mapper: JsonMapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .build()

    println(mapper.writeValueAsString(t))
    println(mapper.writeValueAsBytes(t).mkString("Array(", ", ", ")"))
  }
}
