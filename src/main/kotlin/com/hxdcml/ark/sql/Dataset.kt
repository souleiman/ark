package com.hxdcml.ark.sql

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.KeyValueGroupedDataset

/**
 * Author: Soul
 * Date: 3/1/2018
 */

object Dataset {
    inline fun <T, reified R> Dataset<T>.smartAs(encoder: Encoder<R> = Encoders.bean(R::class.java)): Dataset<R> {
        return this.`as`(encoder)
    }

    inline fun <T, reified R> Dataset<T>.map(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (T) -> R
    ): Dataset<R> {
        return this.map({ body(it) }, encoder)
    }

    inline fun <T, reified R> Dataset<T>.flatMap(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (T) -> Iterable<R>
    ): Dataset<R> {
        return this.flatMap({ body(it).iterator() }, encoder)
    }

    inline fun <T, reified R> Dataset<T>.groupByKey(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (T) -> R
    ): KeyValueGroupedDataset<R, T> {
        return this.groupByKey({ body(it) }, encoder)
    }

    inline fun <T, reified R> Dataset<T>.mapPartitions(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (Iterator<T>) -> Iterable<R>
    ): Dataset<R> {
        return this.mapPartitions({ body(it).iterator() }, encoder)
    }
}