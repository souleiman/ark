package com.hxdcml.ark.sql

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.KeyValueGroupedDataset
import java.io.Serializable

/**
 * Author: Soul
 * Date: 3/1/2018
 */

object KeyValueGroupedDataset {
    inline fun <K, V, reified R> KeyValueGroupedDataset<K, V>.flatMapGroups(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (K, Iterator<V>) -> Iterable<R>
    ): Dataset<R> {
        return this.flatMapGroups({ key, values -> body(key, values).iterator() }, encoder)
    }
}

class ArkKeyValueGroupedDataset<K, V>(val dataset: KeyValueGroupedDataset<K, V>) : Serializable {
    inline fun <reified R> flatMapGroups(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (K, Iterator<V>) -> Iterable<R>
    ): ArkDataset<R> {
        val function: (K, MutableIterator<V>) -> Iterator<R> = { key, values -> body(key, values).iterator() }
        val process = dataset.flatMapGroups(function, encoder)
        return ArkDataset(process)
    }
}