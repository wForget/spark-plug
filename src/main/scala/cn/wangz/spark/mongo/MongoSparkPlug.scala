package cn.wangz.spark.mongo

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.bson.Document

import scala.collection.JavaConverters._

/**
  * Created by hadoop on 2019/3/1.
  *
  * 由于 MongoSpark 没有 upsert 的操作（只有基于 ID 的 upsert），根据 MongoSpark 源码，写了如下的适配
  *
  */
object MongoSparkPlug {
  def bulkUpsert(rdd: RDD[UpdateOneModel[Document]], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
        iter.grouped(writeConfig.maxBatchSize).foreach(batch => {
          collection.bulkWrite(batch.toList.asJava)
        })
      })
    })
  }
}