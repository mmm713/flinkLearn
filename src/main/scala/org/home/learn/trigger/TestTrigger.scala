package org.home.learn.trigger

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class TestTrigger[T] extends Trigger[T, TimeWindow]{
  override def onElement(t: T, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = ???

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = ???

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = ???

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = ???
}
