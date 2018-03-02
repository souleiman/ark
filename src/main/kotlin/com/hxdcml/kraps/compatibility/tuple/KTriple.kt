package com.hxdcml.kraps.compatibility.tuple

import scala.Tuple3

/**
 * Author: Soul
 * Date: 3/2/2018
 */
data class KTriple<U, V, R>(val first: U, val second: V, val third: R) : Tuple3<U, V, R>(first, second, third) {
    object Hook {
        fun <U, V, R> Tuple3<U, V, R>.toTriple() = KTriple(this._1(), this._2(), this._3())
        operator fun <U, V, R> Tuple3<U, V, R>.component1(): U = this._1()
        operator fun <U, V, R> Tuple3<U, V, R>.component2(): V = this._2()
        operator fun <U, V, R> Tuple3<U, V, R>.component3(): R = this._3()
    }
}