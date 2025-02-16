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
import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer
import org.bdgenomics.adam.models.Interval

// k = type for entity id, T = value type stored in hashmap
class IntervalTree[K <: Interval, T: ClassTag] extends Serializable {
  var root: Node[K, T] = null
  var leftDepth: Long = 0
  var rightDepth: Long = 0
  val threshold = 15
  var nodeCount: Long = 0

  def this(nodes: List[Node[K,T]]) = {
    this
    this.insertRecursive(nodes)
  }

  def snapshot(): IntervalTree[K, T] = {
    val newTree: IntervalTree[K, T] = new IntervalTree[K, T]()
    val nodes: List[Node[K, T]] = inOrder()
    newTree.insertRecursive(nodes)
    newTree
  }

  def get(): List[(K, T)] = {
    inOrder().flatMap(r => r.get.toList)
  }

  def countNodes(): Long = {
    nodeCount
  }

  def size(): Long = {
    count
  }

  def merge(nT: IntervalTree[K, T]): IntervalTree[K, T] = {
    val newNodes: List[Node[K, T]] = nT.inOrder()
    val newTree = this.snapshot()
    newTree.insertRecursive(newNodes)
    newTree
  }

  def printNodes(): Unit = {
    println("Printing all nodes in interval tree")
    val nodes: List[Node[K, T]] = inOrder().sortWith(_.getInterval.start < _.getInterval.start)
    nodes.foreach(r => {
      println(r.getInterval)
      r.data.foreach(e => println(e))
    })
  }

  def insert(r: K, v: T) = {
    insert(r, Iterator(v))
  }

  def insert(r: K, vs: Iterator[T]) = {
    insertRegion(r, vs)
    if (Math.abs(leftDepth - rightDepth) > threshold) {
      rebalance()
    }
  }

  def insert(kvs: Iterator[(K, T)])= {
    val grouped = kvs.toList.groupBy(_._1)
    grouped.map(kv => insertRegion(kv._1, kv._2.map(_._2).toIterator))
    if (Math.abs(leftDepth - rightDepth) > threshold) {
      rebalance()
    }
  }

  /*
  * This method finds an existing node (keyed by Interval) to insert the data into,
  * or creates a new node to insert it into the tree
  */
  private def insertRegion(interval: K, vs: Iterator[T]) = {
    if (root == null) {
      nodeCount += 1
      root = new Node[K, T](interval)
      root.multiput(vs)
    }
    var curr: Node[K, T] = root
    var parent: Node[K, T] = null
    var search: Boolean = true
    var leftSide: Boolean = false
    var rightSide: Boolean = false
    var tempLeftDepth: Long = 0
    var tempRightDepth: Long = 0

    while (search) {
      curr.subtreeMax = Math.max(curr.subtreeMax, interval.end)
      parent = curr
      if (curr.greaterThan(interval)) { //left traversal
        if (!leftSide && !rightSide) {
          leftSide = true
        }
        tempLeftDepth += 1
        curr = curr.leftChild
        if (curr == null) {
          curr = new Node(interval)
          curr.multiput(vs)
          parent.leftChild = curr
          nodeCount += 1
          search = false
        }
      } else if (curr.lessThan(interval)) { //right traversal
        if (!leftSide && !rightSide) {
          rightSide = true
        }
        tempRightDepth += 1
        curr = curr.rightChild
        if (curr == null) {
          curr = new Node(interval)
          curr.multiput(vs)
          parent.rightChild= curr
          nodeCount += 1
          search = false
        }
      } else { // insert new id, given id is not in tree
        curr.multiput(vs)
        search = false
      }
    }
    // done searching, set our max depths
    if (tempLeftDepth > leftDepth) {
      leftDepth = tempLeftDepth
    } else if (tempRightDepth > rightDepth) {
      rightDepth = tempRightDepth
    }
  }

  /* searches for single interval over single id */
  def search(r: K): Iterator[(K, T)] = {
    search(r, root)
  }

  /**
   * Used for map
   */
  def mapValues[T2: ClassTag](f: T => T2): IntervalTree[K, T2] = {
    val mappedList: List[Node[K, T2]] =
      inOrder.map(elem => {
        Node(elem.getInterval, elem.data.map(f))
      })
    new IntervalTree[K, T2](mappedList)

  }

  /**
   * Constructs a new tree by applying a predicate over the existing tree
   * for both keys and values
   */
  def filter(pred: (K, T) => Boolean): IntervalTree[K, T] = {
      val filteredNodes = inOrder // TODO: filter by node .filter(r => r.overlaps)
          .map(node => {
            val mapped:Array[T] = node.data.map(r => (node.getInterval, r))
                     .filter(r => pred(r._1, r._2)).map(_._2)
            new Node(node.getInterval, mapped)
          }).filter(!_.data.isEmpty)
    new IntervalTree[K, T](filteredNodes)
  }


  private def search(r: K, n: Node[K, T]): Iterator[(K, T)] = {
    val results = new ListBuffer[(K, T)]()
    if (n != null) {
      if (n.overlaps(r)) {
        results ++= n.get
      }
      if (n.subtreeMax < r.start) {
        return results.distinct.toIterator
      }
      if (n.leftChild != null) {
        results ++= search(r, n.leftChild)
      }
      if (n.rightChild != null) {
        results ++= search(r, n.rightChild)
      }
    }
    return results.distinct.toIterator
  }


  /*
  * This method is used for bulk insertions of Nodes into a tree,
  * specifically with regards to rebalancing
  * Note: this method only appends data to existing nodes if a node with the
  *   same exact Interval exists. In insertRegion, it will insert the data
  *   if the Interval is a subregion of a particular Node.
  */
  def insertNode(n: Node[K, T]): Boolean = {
   if (root == null) {
      root = n
      nodeCount += 1
      return true
    }
    var curr: Node[K, T] = root
    var parent: Node[K, T] = null
    var search: Boolean = true
    var leftSide: Boolean = false
    var rightSide: Boolean = false
    var tempLeftDepth: Long = 0
    var tempRightDepth: Long = 0
    while (search) {
      curr.subtreeMax = Math.max(curr.subtreeMax, n.getInterval.end)
      parent = curr
      if (curr.greaterThan(n.getInterval)) { //left traversal
        if (!leftSide && !rightSide) {
          leftSide = true
        }
        tempLeftDepth += 1
        curr = curr.leftChild
        if (curr == null) {
          parent.leftChild = n
          nodeCount += 1
          search = false
        }
      } else if (curr.lessThan(n.getInterval)) { //right traversal
        if (!leftSide && !rightSide) {
          rightSide = true
        }
        tempRightDepth += 1
        curr = curr.rightChild
        if (curr == null) {
          parent.rightChild= n
          nodeCount += 1
          search = false
        }
      } else { // attempting to replace a node already in tree. Merge
        curr.multiput(n.get().map(_._2))
        search = false
      }
    }
    // done searching, now let's set our max depths
    if (tempLeftDepth > leftDepth) {
      leftDepth = tempLeftDepth
    } else if (tempRightDepth > rightDepth) {
      rightDepth = tempRightDepth
    }
    true
  }

  private def insertRecursive(nodes: List[Node[K, T]]): Unit = {
    if (nodes == null) {
      return
    }
    if (!nodes.isEmpty) {
      val count = nodes.length
      val middle = count/2
      val node = nodes(middle)

      insertNode(node)
      insertRecursive(nodes.take(middle))
      insertRecursive(nodes.drop(middle + 1))
    }
  }


  // currently gets an inorder list of the tree, then bulk constructs a new tree
  private def rebalance() = {
    val nodes: List[Node[K, T]] = inOrder()
    root = null
    nodeCount = 0
    val orderedList = nodes.sortWith(_.getInterval.start < _.getInterval.start)
    orderedList.foreach(n => n.clearChildren())
    insertRecursive(orderedList)
  }

  private def inOrder(): List[Node[K, T]] = {
    return inOrder(root).toList
  }

  private def count(): Long = {
    count(root)
  }

  private def count(n: Node[K, T]): Long = {
    var total: Long = 0
    if (n == null) {
      return total
    }
    total += n.getSize
    total += count(n.leftChild)
    total += count(n.rightChild)
    total
  }

  private def inOrder(n: Node[K, T]): List[Node[K, T]] = {
    if (n == null) {
      return List.empty[Node[K, T]]
    }

    val seen = new ListBuffer[Node[K, T]]()
    seen += n.clone

    seen ++= inOrder(n.leftChild)
    seen ++= inOrder(n.rightChild)
    seen.toList
  }

}
