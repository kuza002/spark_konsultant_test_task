package com.test.task.config


case class AnalyzerConfig(sparkSessionName: String,
                          filesPath: String,
                          targetValue: String,
                          encoding: String,
                          outputPath: String,
                          logsPath: String)

case class SparkConfig(hostName: String)
