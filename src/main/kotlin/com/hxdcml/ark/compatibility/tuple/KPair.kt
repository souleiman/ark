package com.hxdcml.ark.compatibility.tuple

import scala.Tuple2

/**
 * Author: Soul
 * Date: 3/2/2018
 */

data class KPair<U, V>(val first: U, val second: V) : Tuple2<U, V>(first, second) {
    object Hook {
        fun <U, V> Tuple2<U, V>.toPair() = KPair(this._1(), this._2())
        operator fun <U, V> Tuple2<U, V>.component1(): U = this._1()
        operator fun <U, V> Tuple2<U, V>.component2(): V = this._2()
    }
}