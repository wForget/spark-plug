package cn.wangz.spark.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StructType}

object RowUtils {

  def mapToRow(m: Map[String, Any], schema: StructType): Row = Row.fromSeq(m.toList.map {
    case (key, struct: Map[String, Any]@unchecked) =>
      schema.fieldIndex(key) -> mapToRow(struct, getStructTypeFromStructType(key, schema))
    case (key, mapList) if mapList.isInstanceOf[TraversableOnce[_]]
      && mapList.asInstanceOf[TraversableOnce[Any]].toSeq.headOption.exists(_.isInstanceOf[Map[_, _]]) =>
      schema.fieldIndex(key) ->
        mapList.asInstanceOf[TraversableOnce[Any]]
          .toSeq
          .map(_.asInstanceOf[Map[String, Any]])
          .map(mapToRow(_, getStructTypeFromArrayType(key, schema)))
    case (key, None) =>
      schema.fieldIndex(key) -> null
    case (key, Some(other: Map[_, _])) =>
      schema.fieldIndex(key) -> mapToRow(other.asInstanceOf[Map[String, Any]], getStructTypeFromStructType(key, schema))
    case (key, Some(mapList))
      if mapList.isInstanceOf[TraversableOnce[_]]
        && mapList.asInstanceOf[TraversableOnce[Any]].toSeq.headOption.exists(_.isInstanceOf[Map[_, _]]) =>
      schema.fieldIndex(key) ->
        mapList.asInstanceOf[TraversableOnce[Any]]
          .toSeq
          .map(_.asInstanceOf[Map[String, Any]])
          .map(mapToRow(_, getStructTypeFromArrayType(key, schema)))
    case x@(key, Some(other)) =>
      schema.fieldIndex(key) -> other
    case (key, other) =>
      schema.fieldIndex(key) -> other
  }.sortBy(_._1).map(_._2))

  def getStructTypeFromStructType(field: String, schema: StructType): StructType =
    schema.fields(schema.fieldIndex(field)).dataType.asInstanceOf[StructType]

  def getStructTypeFromArrayType(field: String, schema: StructType): StructType =
    schema.fields(schema.fieldIndex(field)).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]

}
