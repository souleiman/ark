package com.hxdcml.ark.compatibility.tuple

import com.hxdcml.ark.compatibility.tuple.KTriple.Hook.component1
import com.hxdcml.ark.compatibility.tuple.KTriple.Hook.component2
import com.hxdcml.ark.compatibility.tuple.KTriple.Hook.component3
import com.hxdcml.ark.compatibility.tuple.KTriple.Hook.toTriple
import org.amshove.kluent.shouldBeInstanceOf
import org.amshove.kluent.shouldEqual
import org.junit.Test

import org.junit.Before
import scala.Tuple3

/**
 * Author: Soul
 * Date: 3/2/2018
 */
class KTripleTest {
    lateinit var triple: KTriple<String, Int, Double>

    @Before
    fun setUp() {
        triple = KTriple("Triple", 3, 3.3333)
    }

    @Test
    fun getFirst() {
        triple.first shouldBeInstanceOf String::class
        triple.first shouldEqual "Triple"
    }

    @Test
    fun getSecond() {
        triple.second shouldBeInstanceOf Int::class
        triple.second shouldEqual 3
    }

    @Test
    fun getThird() {
        triple.third shouldBeInstanceOf Double::class
        triple.third shouldEqual 3.3333
    }

    @Test
    operator fun component1() {
        triple.first shouldBeInstanceOf String::class
        triple.first shouldEqual "Triple"
    }

    @Test
    operator fun component2() {
        triple.second shouldBeInstanceOf Int::class
        triple.second shouldEqual 3
    }

    @Test
    operator fun component3() {
        triple.third shouldBeInstanceOf Double::class
        triple.third shouldEqual 3.3333
    }

    @Test
    fun copy() {
        val copied = triple.copy("Three", 3, 3.14)

        copied.first shouldBeInstanceOf String::class
        copied.first shouldEqual "Three"

        copied.second shouldBeInstanceOf Int::class
        copied.second shouldEqual 3

        copied.third shouldBeInstanceOf Double::class
        copied.third shouldEqual 3.14
    }

    @Test
    fun toPair() {
        val scalaTuple = Tuple3<String, Int, Double>("Negative One", -1, -.9999999999)
        val triple = scalaTuple.toTriple()

        triple.first shouldBeInstanceOf String::class
        triple.first shouldEqual "Negative One"

        triple.second shouldBeInstanceOf Int::class
        triple.second shouldEqual -1

        triple.third shouldBeInstanceOf Double::class
        triple.third shouldEqual -.9999999999
    }

    @Test
    fun hookComponents() {
        val (first, second, third) = Tuple3<String, Int, Double>("Negative One", -1, -.9999999999)

        first shouldBeInstanceOf String::class
        first shouldEqual "Negative One"

        second shouldBeInstanceOf Int::class
        second shouldEqual -1

        third shouldBeInstanceOf Double::class
        third shouldEqual -.9999999999
    }
}