package com.rayanral

import org.apache.spark.sql._

class Accelerator(sparkSession: SparkSession) {

  import sparkSession.implicits._

  def redGoezFasta(units: Dataset[WarhammerUnit]): Dataset[WarhammerUnit] = {
    units.map { unit =>
      unit.color match {
        case "Red" => unit.copy(movement = unit.movement + 5)
        case _ => unit
      }
    }
  }

}
