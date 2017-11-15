package com.krisdrum.utils

object FileUtils {
  def fileName(name: String) = s"$name-${Timestamp.now}"
}
