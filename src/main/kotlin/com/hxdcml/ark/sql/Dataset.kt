package com.hxdcml.ark.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.KeyValueGroupedDataset
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils.random
import java.io.Serializable

/**
 * Author: Soul
 * Date: 3/1/2018
 */

object Dataset {
    inline fun <T, reified R> Dataset<T>.smartAs(
        encoder: Encoder<R> = Encoders.bean(R::class.java)
    ): Dataset<R> {
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

typealias ArkDataFrame = ArkDataset<Row>

class ArkDataset<T>(val dataset: Dataset<T>) : Serializable {
    val columns get() = dataset.columns()
    val first get() = dataset.head()
    val count get() = dataset.count()
    val storage get() = dataset.storageLevel()
    val rdd get() = dataset.javaRDD()
    val write get() = dataset.write()
    val stream get() = dataset.writeStream()
    val inputs get() = dataset.inputFiles()
    val isLocal get() = dataset.isLocal
    val session get() = dataset.sparkSession()
    val context get() = dataset.sqlContext()

    fun toDF() = ArkDataFrame(dataset.toDF())

    fun toDF(vararg columns: String) = ArkDataFrame(dataset.toDF(*columns))

    fun toJSON() = ArkDataset(dataset.toJSON())

    operator fun get(column: String): Column = dataset.apply(column)

    fun colRegex(regex: String): Column = dataset.colRegex(regex)

    fun sortWithinPartition(column: String, vararg columns: String): ArkDataset<T> {
        return ArkDataset(dataset.sortWithinPartitions(column, *columns))
    }

    fun sortWithinPartition(vararg columns: Column): ArkDataset<T> {
        return ArkDataset(dataset.sortWithinPartitions(*columns))
    }

    fun sort(column: String, vararg columns: String): ArkDataset<T> {
        return ArkDataset(dataset.sort(column, *columns))
    }

    fun sort(vararg columns: Column): ArkDataset<T> {
        return ArkDataset(dataset.sort(*columns))
    }

    fun orderBy(column: String, vararg columns: String): ArkDataset<T> {
        return ArkDataset(dataset.orderBy(column, *columns))
    }

    fun orderBy(vararg columns: Column): ArkDataset<T> {
        return ArkDataset(dataset.orderBy(*columns))
    }

    fun hint(column: String, vararg parameters: Any): ArkDataset<T> {
        return ArkDataset(dataset.hint(column, *parameters))
    }

    fun select(column: String, vararg columns: String): ArkDataFrame {
        return ArkDataFrame(dataset.select(column, *columns))
    }

    fun select(vararg columns: Column): ArkDataFrame {
        return ArkDataFrame(dataset.select(*columns))
    }

    fun selectExpr(vararg expressions: String): ArkDataFrame {
        return ArkDataFrame(dataset.selectExpr(*expressions))
    }

    fun groupBy(vararg columns: Column): ArkRelationalGroupedDataset {
        return ArkRelationalGroupedDataset(dataset.groupBy(*columns))
    }

    fun groupBy(column: String, vararg columns: String): ArkRelationalGroupedDataset {
        return ArkRelationalGroupedDataset(dataset.groupBy(column, *columns))
    }

    fun rollup(vararg columns: Column): ArkRelationalGroupedDataset {
        return ArkRelationalGroupedDataset(dataset.rollup(*columns))
    }

    fun rollup(column: String, vararg columns: String): ArkRelationalGroupedDataset {
        return ArkRelationalGroupedDataset(dataset.rollup(column, *columns))
    }

    fun cube(vararg columns: Column): ArkRelationalGroupedDataset {
        return ArkRelationalGroupedDataset(dataset.cube(*columns))
    }

    fun cube(column: String, vararg columns: String): ArkRelationalGroupedDataset {
        return ArkRelationalGroupedDataset(dataset.cube(column, *columns))
    }

    fun agg(column: Column, vararg columns: Column) = ArkDataFrame(dataset.agg(column, *columns))

    fun drop(vararg columns: String) = ArkDataFrame(dataset.drop(*columns))

    fun drop(column: Column) = ArkDataFrame(dataset.drop(column))

    fun drop(column: String) = ArkDataFrame(dataset.drop(column))

    fun dropDuplicates() = ArkDataset(dataset.dropDuplicates())

    fun dropDuplicates(columns: Array<out String>) = ArkDataset(dataset.dropDuplicates(columns))

    fun dropDuplicates(column: String, vararg columns: String) = ArkDataset(dataset.dropDuplicates(column, *columns))

    fun describe() = ArkDataFrame(dataset.describe())

    fun describe(vararg columns: String) = ArkDataFrame(dataset.describe(*columns))

    fun summary() = ArkDataFrame(dataset.summary())

    fun summary(vararg columns: String) = ArkDataFrame(dataset.describe(*columns))

    fun <R> transform(body: (ArkDataset<T>) -> (ArkDataset<R>)): ArkDataset<R> {
        return body(this)
    }

    fun filter(predicate: (T) -> Boolean) = ArkDataset(dataset.filter({ predicate(it) }))

    fun filter(predicate: Column) = ArkDataset(dataset.filter(predicate))

    fun filter(predicate: String) = ArkDataset(dataset.filter(predicate))

    inline fun <reified R> smartAs(encoder: Encoder<R> = Encoders.bean(R::class.java)): ArkDataset<R> {
        val process = dataset.`as`(encoder)
        return ArkDataset(process)
    }

    fun reduce(reducer: (T, T) -> T): T {
        return dataset.reduce({ a, b -> reducer(a, b) })
    }

    inline fun <reified R> map(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (T) -> R
    ): ArkDataset<R> {
        val process = dataset.map({ body(it) }, encoder)
        return ArkDataset(process)
    }

    inline fun <reified R> flatMap(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (T) -> Iterable<R>
    ): ArkDataset<R> {
        val process = dataset.flatMap({ body(it).iterator() }, encoder)
        return ArkDataset(process)
    }

    inline fun <reified R> groupByKey(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (T) -> R
    ): ArkKeyValueGroupedDataset<R, T> {
        val process = dataset.groupByKey({ body(it) }, encoder)
        return ArkKeyValueGroupedDataset(process)
    }

    inline fun <reified R> mapPartitions(
        encoder: Encoder<R> = Encoders.bean(R::class.java),
        crossinline body: (Iterator<T>) -> Iterable<R>
    ): ArkDataset<R> {
        val process = dataset.mapPartitions({ body(it).iterator() }, encoder)
        return ArkDataset(process)
    }

    fun foreach(consumer: (T) -> Unit) = dataset.foreach({ consumer(it) })

    fun take(n: Int = 1): List<T> = dataset.takeAsList(n)

    fun collect(): List<T> = dataset.collectAsList()

    fun limit(n: Int = 1) = ArkDataset(dataset.limit(n))

    operator fun iterator(): Iterator<T> = dataset.toLocalIterator()

    fun repartition(partitions: Int): ArkDataset<T> {
        return ArkDataset(dataset.repartition(partitions))
    }

    fun repartition(partitions: Int, vararg column: Column): ArkDataset<T> {
        return ArkDataset(dataset.repartition(partitions, *column))
    }

    fun repartitionByRange(partitions: Int): ArkDataset<T> {
        return ArkDataset(dataset.repartitionByRange(partitions))
    }

    fun repartitionByRange(partitions: Int, vararg column: Column): ArkDataset<T> {
        return ArkDataset(dataset.repartitionByRange(partitions, *column))
    }

    fun coalesce(partitions: Int) = ArkDataset(dataset.coalesce(partitions))

    fun distinct() = ArkDataset(dataset.distinct())

    fun persist() = ArkDataset(dataset.persist())

    fun unpersist() = ArkDataset(dataset.unpersist())

    fun persist(level: StorageLevel) = ArkDataset(dataset.persist(level))

    fun unpersist(blocking: Boolean) = ArkDataset(dataset.unpersist(blocking))

    fun cache() = ArkDataset(dataset.cache())

    fun checkpoint() = ArkDataset(dataset.checkpoint())

    fun checkpoint(eager: Boolean) = ArkDataset(dataset.checkpoint(eager))

    fun localCheckpoint() = ArkDataset(dataset.localCheckpoint())

    fun localCheckpoint(eager: Boolean) = ArkDataset(dataset.localCheckpoint(eager))

    fun createTempView(name: String) = dataset.createTempView(name)

    fun createOrReplaceTempView(name: String) = dataset.createOrReplaceTempView(name)

    fun createGlobalTempView(name: String) = dataset.createGlobalTempView(name)

    fun createOrReplaceGlobalTempView(name: String) = dataset.createOrReplaceGlobalTempView(name)

    fun types(): List<Pair<String, String>> {
        val types = dataset.dtypes()
        return types.map { it._1 to it._2 }
    }

    fun explain() = dataset.explain()

    fun explain(extended: Boolean) = dataset.explain(extended)

    fun except(other: Dataset<T>) = ArkDataset(dataset.except(other))

    fun except(other: ArkDataset<T>) = this.except(other.dataset)

    operator fun minus(other: Dataset<T>) = this.except(other)

    operator fun minus(other: ArkDataset<T>) = this.except(other)

    fun intersect(other: Dataset<T>) = ArkDataset(dataset.intersect(other))

    fun intersect(other: ArkDataset<T>) = this.intersect(other.dataset)

    fun union(other: Dataset<T>) = ArkDataset(dataset.union(other))

    fun union(other: ArkDataset<T>) = this.union(dataset.union(other.dataset))

    fun unionByName(other: Dataset<T>) = ArkDataset(dataset.unionByName(other))

    fun unionByName(other: ArkDataset<T>) = this.unionByName(dataset.unionByName(other.dataset))

    fun alias(alias: String) = ArkDataset(dataset.alias(alias))

    fun crossJoin(right: Dataset<T>): ArkDataFrame = ArkDataFrame(dataset.crossJoin(right))

    fun crossJoin(right: ArkDataset<T>) = this.crossJoin(right.dataset)

    infix fun cross(right: Dataset<T>) = this.crossJoin(right)

    infix fun cross(right: ArkDataset<T>) = this.crossJoin(right)

    fun join(right: Dataset<T>) = ArkDataFrame(dataset.join(right))

    fun join(right: ArkDataset<T>) = this.join(right.dataset)

    fun join(right: Dataset<T>, expression: Column) = ArkDataFrame(dataset.join(right, expression))

    fun join(right: ArkDataset<T>, expression: Column) = this.join(right.dataset, expression)

    fun join(right: Dataset<T>, expression: Column, type: String) = ArkDataFrame(dataset.join(right, expression, type))

    fun join(right: ArkDataset<T>, expression: Column, type: String) = this.join(right.dataset, expression, type)

    fun printSchema() = dataset.printSchema()

    fun schema() = dataset.schema()

    fun show(num: Int = 20, truncate: Boolean = false) = dataset.show(num, truncate)

    fun show(num: Int = 20, truncate: Boolean = false, predicate: (T) -> Boolean) {
        this.filter(predicate).show(num, truncate)
    }

    fun queryExecution() = dataset.queryExecution()

    fun randomSplit(weights: DoubleArray, seed: Long = random().nextLong()): List<ArkDataset<T>> {
        return dataset.randomSplit(weights, seed).map { ArkDataset(it) }
    }

    fun sample(fraction: Double, seed: Long = random().nextLong()): ArkDataset<T> {
        return ArkDataset(dataset.sample(fraction, seed))
    }

    fun sample(replace: Boolean = false, fraction: Double, seed: Long = random().nextLong()): ArkDataset<T> {
        return ArkDataset(dataset.sample(replace, fraction, seed))
    }

    fun stat() = dataset.stat()

    fun na() = dataset.na()

    fun withColumn(name: String, column: Column) = ArkDataFrame(dataset.withColumn(name, column))

    fun withColumnRenamed(old: String, new: String) = ArkDataFrame(dataset.withColumnRenamed(old, new))

    fun withWatermark(event: String, delay: String) = ArkDataset(dataset.withWatermark(event, delay))
}