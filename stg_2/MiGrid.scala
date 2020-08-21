import org.apache.spark.sql.types._


object MiGrid extends Enumeration {
  val SQUARE_ID,
  X0, Y0,
  X1, Y1,
  X2, Y2,
  X3, Y3,
  X4, Y4 = Value

  val structType = StructType(
    Seq(
      StructField(SQUARE_ID.toString, IntegerType),
      StructField(X0.toString, DoubleType), StructField(Y0.toString, DoubleType),
      StructField(X1.toString, DoubleType), StructField(Y1.toString, DoubleType),
      StructField(X3.toString, DoubleType), StructField(Y3.toString, DoubleType),
      StructField(X4.toString, DoubleType), StructField(Y4.toString, DoubleType)
    )
  )
}
