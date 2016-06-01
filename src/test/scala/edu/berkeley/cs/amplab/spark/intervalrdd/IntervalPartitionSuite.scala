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
F* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.spark.intervalrdd

import org.bdgenomics.adam.models.ReferenceRegion
import org.scalatest.FunSuite

class IntervalPartitionSuite extends FunSuite  {

	val chr = "chr1"
	val region1: ReferenceRegion = new ReferenceRegion(chr, 0L, 99L)
	val region2: ReferenceRegion = new ReferenceRegion(chr, 100L, 199L)
	val region3: ReferenceRegion = new ReferenceRegion(chr, 0, 199)
	val region4: ReferenceRegion = new ReferenceRegion(chr, 150L, 300L)
	val region5: ReferenceRegion = new ReferenceRegion(chr, 350L, 700L)

	val read1 = 2L
	val read2 = 500L
	val read3 = 3L
	val read4 = 700L
	val read5 = 900L
	val read6 = 250L

	test("create new partition") {

		var partition: IntervalPartition[ReferenceRegion, Long] = new IntervalPartition[ReferenceRegion, Long]()
		assert(partition != null)

	}

	test("create partition from iterator") {

		val iter = Iterator((region1, read1), (region2, read1))
		val partition = IntervalPartition(iter)
		assert(partition != null)
	}

	test("get values from iterator-created partition") {

		val iter = Iterator((region1, read1), (region2, read2), (region1, read3), (region2, read4))
		val partition = IntervalPartition(iter)

		var results: List[(ReferenceRegion, Long)] = partition.get(region1).toList
		results = results ++ partition.get(region2).toList

		assert(results.contains((region1, read1)))
		assert(results.contains((region2, read2)))
		assert(results.contains((region1, read3)))
		assert(results.contains((region2, read4)))

	}

	test("put some for iterator of intervals and key-values") {

		var partition: IntervalPartition[ReferenceRegion, Long] = new IntervalPartition[ReferenceRegion, Long]()

		var newPartition = partition.multiput(region1, Iterator(read1, read3))
		newPartition = newPartition.multiput(region2, Iterator(read2, read4))

		// assert values are in the new partition
		var results: List[(ReferenceRegion, Long)] = newPartition.get(region1).toList
		results = results ++ newPartition.get(region2).toList

		assert(results.contains((region1, read1)))
		assert(results.contains((region2, read2)))
		assert(results.contains((region1, read3)))
		assert(results.contains((region2, read4)))

	}

	test("get some for iterator of intervals") {

		val read1 = (1L,2L)
		val read2 = (1L,500L)
		val read3 = (2L, 6L)
		val read4 =  (2L, 500L)
		val iter = Iterator((region1, read1), (region2, read2), (region1, read3), (region2, read4))
		val partition = IntervalPartition(iter)
		var results = partition.get(region1).toList
		assert(results.contains((region1, read1)))

	}

	test("selectively getting intervals") {

		var partition: IntervalPartition[ReferenceRegion, Long] = new IntervalPartition[ReferenceRegion, Long]()

		var newPartition = partition.multiput(region1, Iterator(read1, read3))
		newPartition = newPartition.multiput(region2, Iterator(read2, read4))

		// assert values are in the new partition
		var results: List[(ReferenceRegion, Long)] = newPartition.get(region1).toList

		assert(results.contains((region1, read1)))
		assert(!results.contains((region2, read2)))
		assert(results.contains((region1, read3)))
		assert(!results.contains((region2, read4)))

	}

	test("putting differing number of reads into different regions") {

		var partition: IntervalPartition[ReferenceRegion, Long] = new IntervalPartition[ReferenceRegion, Long]()

		var newPartition = partition.multiput(region1, Iterator(read1, read3))
		newPartition = newPartition.multiput(region2, Iterator(read4, read2, read5))

		// assert values are in the new partition
		var results: List[(ReferenceRegion, Long)] = newPartition.get(region1).toList
		results = results ++ newPartition.get(region2).toList

		assert(results.contains((region1, read1)))
		assert(results.contains((region2, read2)))
		assert(results.contains((region1, read3)))
		assert(results.contains((region2, read4)))
		assert(results.contains((region2, read5)))

	}

	test("putting then getting a region that overlaps 3 regions") {

		var partition: IntervalPartition[ReferenceRegion, Long] = new IntervalPartition[ReferenceRegion, Long]()

		var newPartition = partition.multiput(region1, Iterator(read1, read3))
		newPartition = newPartition.multiput(region2, Iterator(read2, read4))
		newPartition = newPartition.put(region4, read5)
		newPartition = newPartition.put(region5, read6)

		val overlapReg: ReferenceRegion = new ReferenceRegion(chr, 0L, 200L)

		val results = newPartition.get(overlapReg).toList
		assert(results.size == 5)

	}

	test("applying a predicate") {

		var partition: IntervalPartition[ReferenceRegion, Long] = new IntervalPartition[ReferenceRegion, Long]()

		var newPartition = partition.multiput(region1, Iterator(read1, read3))
		newPartition = newPartition.multiput(region2, Iterator(read2, read4))

		val filtPart = newPartition.filter((k, v) => v < 300L)
		val overlapReg: ReferenceRegion = new ReferenceRegion(chr, 0L, 300L)

		val results = filtPart.get(overlapReg).toList
		assert(results.size == 2)

	}

	test("applying a map") {

		var partition: IntervalPartition[ReferenceRegion, Long] = new IntervalPartition[ReferenceRegion, Long]()

		var newPartition = partition.multiput(region1, Iterator(read1, read3))
		newPartition = newPartition.multiput(region2, Iterator(read2, read4))

		val filtPart = newPartition.mapValues(elem => elem + 300L)

		val results = filtPart.filter((k, v) => v < 400L).get
		assert(results.size == 2)

	}

}
