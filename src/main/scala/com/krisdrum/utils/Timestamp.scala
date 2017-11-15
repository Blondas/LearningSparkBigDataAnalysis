package com.krisdrum.utils

import java.text.SimpleDateFormat
import java.util.Date

object Timestamp {
  val simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd-kk.mm.ss")

  def now: String = simpleDateFormat.format(new Date())
}
