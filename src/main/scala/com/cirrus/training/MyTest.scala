package com.cirrus.training

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object MyTest {
  def main(args: Array[String]) {
    val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss")
    val rtn = LocalDateTime.parse("2020-03-27 05:05:05", DATE_TIME_FORMATTER).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    println("rtn:" + rtn)
  }
}
