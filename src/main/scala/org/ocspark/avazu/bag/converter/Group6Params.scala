package org.ocspark.avazu.bag.converter

import org.ocspark.avazu.AbstractParams

case class Group6Params(
  gc_begin: Int = 16,
  max_occur: Int = 100,
  max_sz_group: Int = 100,
  max_nr_group_feats: Int = 2500,
  g_field: String = null,
  a_field: String = null,
  train: String = null, // tr.r{size}.{category}.new.csv
  valid: String = null,
  partition: String = null) extends AbstractParams[Group6Params] 