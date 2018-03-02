package com.hxdcml.kraps.sql

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.KeyValueGroupedDataset

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