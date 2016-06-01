/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.spark.intervalrdd

import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, Partition, TaskContext, _}
import org.bdgenomics.adam.models.Interval

import scala.reflect.ClassTag

class IntervalRDD[K<: Interval: ClassTag, V: ClassTag](
    /** The underlying representation of the IndexedRDD as an RDD of partitions. */
    private val partitionsRDD: RDD[IntervalPartition[K, V]])
  extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) with Logging {

  require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /** Provides the `RDD[(K, V)]` equivalent output. */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    // This needs to be present to compile
    null
  }

  def toRDD: RDD[(K, V)] = {
    partitionsRDD.flatMap(_.get)
  }

  override def setName(_name: String): this.type = {
    partitionsRDD.setName(_name)
    this
  }

  /** Persists the interval partitions using `targetStorageLevel`, which defaults to MEMORY_ONLY. */
  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }
  /** Unpersists the interval partitions using */
  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /** The number of elements in the RDD. */
  override def count: Long = {
    partitionsRDD.map(_.getTree.size).reduce(_ + _)
  }

  override def collect: Array[(K, V)] = partitionsRDD.flatMap(r => r.get()).collect


  def filterByInterval(r: K): IntervalRDD[K, V] = {
    mapIntervalPartitions(_.filterByInterval(r))
  }


  /**
   * Performs filtering given a predicate
   */
  override def filter(pred: Tuple2[K,V] => Boolean): IntervalRDD[K, V] = {
    this.mapIntervalPartitions(_.filter(Function.untupled(pred)))
  }

  /**
    * Maps each value, preserving the index.
    */
  def mapValues[V2: ClassTag](f: V => V2): IntervalRDD[K, V2] = {
    this.mapIntervalPartitions(_.mapValues(f))
  }

  /**
   * Maps interval partitions in partitionsRDD
   * @param f mapping function for interval partitions
   * @tparam K2 key type extending Interval
   * @tparam V2 data type
   * @return new IntervalRDD of type [K2, V2]
   */
  def mapIntervalPartitions[K2 <: Interval: ClassTag, V2: ClassTag](
                            f: (IntervalPartition[K, V]) => IntervalPartition[K2, V2]): IntervalRDD[K2, V2] = {

    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    new IntervalRDD(newPartitionsRDD)
  }

  private def withPartitionsRDD[K2 <: Interval: ClassTag, V2: ClassTag](
      partitionsRDD: RDD[IntervalPartition[K2, V2]]): IntervalRDD[K2, V2] = {
    new IntervalRDD(partitionsRDD)
  }

  /** Gets the values corresponding to the specified key, if any
  * Assume that we're only getting data that exists (if it doesn't exist,
  * would have been handled by upper LazyMaterialization layer
  */
  def get(region: K): List[(K,V)] = {

    val results: Array[Array[(K, V)]] = {
      context.runJob(partitionsRDD, (context: TaskContext, partIter: Iterator[IntervalPartition[K, V]]) => {
       if (partIter.hasNext) {
          val intPart = partIter.next()
          intPart.get(region).toArray
       } else {
          Array[(K, V)]()
       }
      })
    }
    results.flatten.toList
  }

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD
   **/
  def multiput(elems: Array[(K, V)]): IntervalRDD[K, V] = {
    val partitioned: RDD[(K, V)] = context.parallelize(elems.toSeq).partitionBy(partitioner.get)

    val convertedPartitions: RDD[IntervalPartition[K, V]] = partitioned.mapPartitions[IntervalPartition[K, V]](
      iter => Iterator(IntervalPartition(iter)),
      preservesPartitioning = true)

    // merge the new partitions with existing partitions
    val merger = new PartitionMerger[K, V]()
    val newPartitionsRDD = partitionsRDD.zipPartitions(convertedPartitions, true)((aiter, biter) => merger(aiter, biter))
    new IntervalRDD(newPartitionsRDD)
  }

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD
   **/
  def multiput(elem: (K, V)): IntervalRDD[K, V] = multiput(Array(elem))

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD
   **/
  def multiput(elems: RDD[(K, V)]): IntervalRDD[K, V] = {
    val partitioned =
      if (elems.partitioner.isDefined) elems
      else {
        elems.partitionBy(new HashPartitioner(elems.partitions.size))
      }

    val convertedPartitions: RDD[IntervalPartition[K, V]] = partitioned.mapPartitions[IntervalPartition[K, V]](
      iter => Iterator(IntervalPartition(iter)),
      preservesPartitioning = true)

    // merge the new partitions with existing partitions
    val merger = new PartitionMerger[K, V]()
    val newPartitionsRDD = partitionsRDD.zipPartitions(convertedPartitions, true)((aiter, biter) => merger(aiter, biter))
    new IntervalRDD(newPartitionsRDD)
  }
}


class PartitionMerger[K <: Interval, V: ClassTag]() extends Serializable {
  def apply(thisIter: Iterator[IntervalPartition[K, V]], otherIter: Iterator[IntervalPartition[K, V]]): Iterator[IntervalPartition[K, V]] = {
    val thisPart = thisIter.next
    val otherPart = otherIter.next
    Iterator(thisPart.mergePartitions(otherPart))
  }
}

object IntervalRDD extends Logging {

  /**
  * Constructs an IntervalRDD from a set of ReferenceRegion, V tuples
  */
  def apply[K <: Interval: ClassTag, V: ClassTag](elems: RDD[(K, V)]) : IntervalRDD[K, V] = {
    val partitioned =
      if (elems.partitioner.isDefined) elems
      else {
        elems.partitionBy(new HashPartitioner(elems.partitions.size))
      }
    val convertedPartitions: RDD[IntervalPartition[K, V]] = partitioned.mapPartitions[IntervalPartition[K, V]](
      iter => Iterator(IntervalPartition(iter)),
      preservesPartitioning = true)

    new IntervalRDD(convertedPartitions)
  }

}
