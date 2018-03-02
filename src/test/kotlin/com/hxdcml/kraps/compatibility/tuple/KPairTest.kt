package com.hxdcml.kraps.compatibility.tuple

import com.hxdcml.kraps.compatibility.tuple.KPair.Hook.toPair
import com.hxdcml.kraps.compatibility.tuple.KPair.Hook.component1
import com.hxdcml.kraps.compatibility.tuple.KPair.Hook.component2
import org.amshove.kluent.shouldBeInstanceOf
import org.amshove.kluent.shouldEqual
import org.junit.Before
import org.junit.Test
import scala.Tuple2


/**
 * Author: Soul
 * Date: 3/2/2018
 */
class KPairTest {
    lateinit var pair: KPair<String, Int>
    @Before
    fun setUp() {
        pair = KPair("Four", 4)
    }

    @Test
    fun getFirst() {
        pair.first shouldBeInstanceOf String::class
        pair.first shouldEqual "Four"
    }

    @Test
    fun getSecond() {
        pair.second shouldBeInstanceOf Int::class
        pair.second shouldEqual 4
    }

    @Test
    fun component1() {
        pair.first shouldBeInstanceOf String::class
        pair.first shouldEqual "Four"
    }

    @Test
    fun component2() {
        pair.second shouldBeInstanceOf Int::class
        pair.second shouldEqual 4
    }

    @Test
    fun copy() {
        val copied = pair.copy("Three", 3)

        copied.first shouldBeInstanceOf String::class
        copied.first shouldEqual "Three"

        copied.second shouldBeInstanceOf Int::class
        copied.second shouldEqual 3
    }

    @Test
    fun toPair() {
        val scalaTuple = Tuple2<String, Int>("Negative One", -1)
        val pair = scalaTuple.toPair()

        pair.first shouldBeInstanceOf String::class
        pair.first shouldEqual "Negative One"

        pair.second shouldBeInstanceOf Int::class
        pair.second shouldEqual -1
    }

    @Test
    fun hookComponents() {
        val (first, second) = Tuple2<String, Int>("Negative One", -1)

        first shouldBeInstanceOf String::class
        first shouldEqual "Negative One"

        second shouldBeInstanceOf Int::class
        second shouldEqual -1
    }
}