package com.cirrus.training

import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.jackson.{JsonMethods, Serialization}

object JsonHelper {
  implicit val formats = DefaultFormats

  def write[T <: AnyRef](value: T): String = Serialization.write(value)

  def parse(json:String):JValue = JsonMethods.parse(json)
}
