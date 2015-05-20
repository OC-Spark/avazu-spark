package org.ocspark.avazu.ensemble.model.app.util

import org.ocspark.avazu.AbstractParams
import scala.reflect.runtime.universe

case class AppRunTemplateParams (
  reg: Double = 0.00002,
  eta: Double = 0.05,
  mark: String = "mark1",
  size: String = null,
  filterString: String = null,
  iter: String = null) extends AbstractParams[AppRunTemplateParams] 
