package com.test.task.service

import com.test.task.util.FilesUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Producer {
  def getRDD(inputPath: String)(implicit spark: SparkSession): RDD[String] = {
    val sessionsPathList = FilesUtils.getListOfFiles(inputPath)
    spark.sparkContext.parallelize(sessionsPathList)
  }
}
