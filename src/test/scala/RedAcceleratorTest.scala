package com.rayanral

import org.scalatest._
import flatspec._
import org.scalatest.matchers.should.Matchers._

class RedAcceleratorTest extends AnyFlatSpec with SparkTester {

  import sparkSession.implicits._

  "accelerator" should "increase movement of red vehicles" in {
    val testData = Seq(
      WarhammerUnit("Orks", "Trukk", "Red", 12),
      WarhammerUnit("Orks", "Trukk", "Blue", 12),
      WarhammerUnit("Blood Angels", "Rhino", "Red", 12),
      WarhammerUnit("Adeptus Astartes", "Librarian", "Ultramarine", 6),
    )
    val testDf = sparkSession.createDataset(testData)
    val accelerator = new Accelerator(sparkSession)
    val resultDf = accelerator.redGoezFasta(testDf)

    resultDf.count() should be(4)
    val redUnits = resultDf.collect().toList.filter(_.color == "Red")
    redUnits should have size(2)
    every(redUnits.map(_.movement)) should be(17)
  }

}
