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

package edu.berkeley.cs.amplab.spark.intervaltree

import java.io.Serializable

import org.bdgenomics.adam.models.Interval

import scala.reflect.ClassTag

class Node[K <: Interval, T: ClassTag](interval: K) extends Serializable {
  var leftChild: Node[K, T] = null
  var rightChild: Node[K, T] = null
  var subtreeMax: Long = interval.end

  // returns interval for this node
  def getInterval: K = interval

  // DATA SHOULD BE STORED MORE EFFICIENTLY
  var data: Array[T] = Array[T]()

  def this(interval: K, data: Array[T]) = {
    this(interval)
    multiput(data)
  }

  def getSize(): Long = {
    data.length
  }

  override def clone: Node[K, T] = {
    val n: Node[K, T] = new Node(interval)
    n.data = data
    n
  }

  def clearChildren() = {
    leftChild = null
    rightChild = null
  }

  def multiput(rs: Array[T]): Unit = {
    val newData = rs
    data ++= newData
  }

  def multiput(rs: Iterator[T]): Unit = {
    multiput(rs.toArray)
  }

  def put(r: T) = {
    multiput(Array(r))
  }

  def get(): Iterator[(K, T)] = data.map(r => (interval, r)).toIterator

  def greaterThan(other: K): Boolean = {
      interval.start > other.start
  }

  def equals(other: K): Boolean = {
      (interval.start == other.start && interval.end == other.end)
  }

  def lessThan(other: K): Boolean = {
      interval.start < other.start
  }

  def overlaps(other: K): Boolean = {
    interval.start < other.end && interval.end > other.start
  }

}

object Node {
  def apply[K <: Interval, T: ClassTag](interval: K, data: Array[T]) = new Node(interval, data)
}
