package org.ocspark.avazu.ensemble.util

import org.ocspark.avazu.AbstractParams
import scala.reflect.runtime.universe

case class RunTemplateParams (
  reg: Double = 0.00002, 
  eta: Double = 0.05, 
  mark: String = "mark1", 
  size: String = null, 
  filterString: String = null, 
  iter: String = null) extends AbstractParams[RunTemplateParams]
 
