package com.test.task.config


case class AnalyzerConfig(sparkSessionName: String,
                          logsPath: String,
                          targetValue: String,
                          encoding: String)

case class SparkConfig(hostName: String)
