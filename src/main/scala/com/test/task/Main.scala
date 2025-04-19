package com.test.task

import com.test.task.service.Producer.getRDD
import com.test.task.service.Step.{step0, step1, step2}
import com.test.task.service.Supplier.printsRows
import com.test.task.util.RDDProcessor.RDDProcessorOps
import com.test.task.util.RDDProducer.RDDProducingOps
import com.test.task.util.RDDSupplier.RDDSupplierOps
import com.test.task.util.SparkResource.usingSparkSession

object Main extends App {

  usingSparkSession("SessionAnalysis") { implicit spark =>

    val inputPath = "./src/main/resources/data/2560"

    val rdd = inputPath.produceRDD(getRDD)
      .transformRDD(step0)
      .transformRDD(step1)
      .transformRDD(step2)
      .cache()

    rdd
      .supply(printsRows)

  }

}
