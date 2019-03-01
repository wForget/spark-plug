package cn.wangz.spark.listener

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

/**
  * Created by hadoop on 2019/3/1.
  *
  * Task 失败监听，失败后发送邮件
  *
  * .config("enableSendEmailOnTaskFail", "true")
  * .config("spark.extraListeners", "cn.i4.monitor.streaming.I4SparkAppListener")
  */
class SparkAppListener (conf: SparkConf) extends SparkListener with Logging {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
    // completion event is for. Let's just drop it here. This means we might have some speculation
    // tasks on the web ui that's never marked as complete.
    if (info != null && taskEnd.stageAttemptId != -1) {
      val errorMessage: Option[String] =
        taskEnd.reason match {
          case kill: TaskKilled =>
            Some(kill.toErrorString)
          case e: ExceptionFailure =>
            Some(e.toErrorString)
          case e: TaskFailedReason =>
            Some(e.toErrorString)
          case _ => None
        }
      if (errorMessage.nonEmpty) {
        if (conf.getBoolean("enableSendEmailOnTaskFail", false)) {
          try {
            // TODO send mail
          } catch {
            case e: Exception =>
          }
        }
      }
    }
  }
}
