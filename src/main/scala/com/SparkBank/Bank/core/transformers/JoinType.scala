package com.SparkBank.Bank.core.transformers

trait JoinType {
  val value:String
}
case object LeftJoin extends JoinType {
  override val value: String = "left"
}
case object RightJoin extends JoinType {
  override val value: String = "right"
}
case object InnerJoin extends JoinType {
  override val value: String = "inner"
}
case object FullJoin extends JoinType {
  override val value: String = "full"
}

