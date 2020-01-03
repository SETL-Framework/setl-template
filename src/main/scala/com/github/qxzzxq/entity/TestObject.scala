package com.github.qxzzxq.entity

import com.jcdecaux.setl.annotation.ColumnName

case class TestObject(@ColumnName("PARTITION_1") partition1: Int,
                      partition2: String,
                      clustering1: String,
                      value: Long)
