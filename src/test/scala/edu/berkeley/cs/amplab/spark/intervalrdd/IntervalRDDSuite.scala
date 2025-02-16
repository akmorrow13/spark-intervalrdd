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

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Logging}
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.GenomicRegionPartitioner
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{AlignmentRecord, Genotype}

import scala.collection.mutable.ListBuffer


class IntervalRDDSuite extends ADAMFunSuite with Logging {

  // create regions
  val chr1 = "chr1"
  val chr2 = "chr2"
  val chr3 = "chr3"
  val region1: ReferenceRegion = new ReferenceRegion(chr1, 0L, 99L)
  val region2: ReferenceRegion = new ReferenceRegion(chr2, 100L, 199L)
  val region3: ReferenceRegion = new ReferenceRegion(chr3, 200L, 299L)

  //creating data
  val rec1 = "data1"
  val rec2 = "data2"
  val rec3 = "data3"
  val rec4 = "data4"
  val rec5 = "data5"
  val rec6 = "data6"

  val sd = new SequenceDictionary(Vector(
    SequenceRecord("chrM", 16299L)))



  sparkTest("Filter and count alignment data over 100 bases") {

    val filePath = "src/test/resources/mouse_chrM.bam"
    val region = new ReferenceRegion("chrM", 0L, 1000L)
    val interval = new ReferenceRegion("chrM", 0L, 100L)

    val rdd: RDD[AlignmentRecord] = sc.loadIndexedBam(filePath, region)

    val alignmentRDD: RDD[(ReferenceRegion, AlignmentRecord)] = rdd.map(v => (ReferenceRegion(v), v)).partitionBy(GenomicRegionPartitioner(10, sd))

    var intRDD: IntervalRDD[ReferenceRegion,  AlignmentRecord] = IntervalRDD(alignmentRDD)
    val filteredRDD = alignmentRDD.filter( r => (r._2.getStart < interval.end && r._2.getEnd > interval.start) )
    val results = intRDD.filterByInterval(interval)
    assert(results.count == filteredRDD.count)
  }

  sparkTest("Get alignment data over 1000 bases") {

    val filePath = "src/test/resources/mouse_chrM.bam"
    val region = new ReferenceRegion("chrM", 0L, 10000L)
    val interval = new ReferenceRegion("chrM", 0L, 1000L)

    val rdd: RDD[AlignmentRecord] = sc.loadIndexedBam(filePath, region)

    val alignmentRDD: RDD[(ReferenceRegion, AlignmentRecord)] = rdd.map(v => (ReferenceRegion(v), v)).partitionBy(GenomicRegionPartitioner(10, sd))

    var intRDD: IntervalRDD[ReferenceRegion,  AlignmentRecord] = IntervalRDD(alignmentRDD)
    val results = intRDD.get(region)

    assert(results.size == rdd.count)
  }

  sparkTest("create IntervalRDD from RDD of datatype string") {

    // parallelize data
    var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr).partitionBy(new HashPartitioner(10))

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var intRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(intArrRDD)
    val results = intRDD.get(region3)

    assert(results.head == (region3, rec3))
    assert(results.size == 1)

  }


  sparkTest("merge new values into existing IntervalRDD") {

    var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(intArrRDD)

    intArr = Array((region2, rec4), (region3, rec5))

    val newRDD: IntervalRDD[ReferenceRegion,  String] = testRDD.multiput(intArr)
    var results = newRDD.get(region3)

    assert(results.size == 2)
  }


  sparkTest("call put multiple times on reads with same ReferenceRegion") {

    val region: ReferenceRegion = new ReferenceRegion("chr1", 0L, 99L)

    var intArr = Array((region, rec1), (region, rec2), (region, rec3))
    var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(intArrRDD)

    //constructing RDDs to put in
    val onePutInput = Array((region, rec4), (region, rec5))
    val twoPutInput = Array((region, rec6))

    // Call Put twice
    val onePutRDD: IntervalRDD[ReferenceRegion,  String] = testRDD.multiput(onePutInput)
    val twoPutRDD: IntervalRDD[ReferenceRegion,  String] = onePutRDD.multiput(twoPutInput)

    var resultsOrig = testRDD.get(region)
    var resultsOne = onePutRDD.get(region)
    var resultsTwo = twoPutRDD.get(region)

    assert(resultsOrig.size == 3) //size of results for the one region we queried
    assert(resultsOne.size == 5) //size after adding two records
    assert(resultsTwo.size == 6) //size after adding another record

  }

  sparkTest("merge RDDs across multiple chromosomes") {

    var intArr = Array((region1, rec1), (region1, rec2), (region1, rec3))
    var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(intArrRDD)

    val arr = Array((region2, rec4), (region2, rec5))

    val chr2RDD: IntervalRDD[ReferenceRegion,  String] = testRDD.multiput(arr)

    var origChr1 = testRDD.get(region1)
    var newChr1 = chr2RDD.get(region1)

    assert(origChr1 == newChr1)

    var newChr2 = chr2RDD.get(region2)

    assert(newChr2.size == arr.size)
  }

  sparkTest("test for vcf data") {

    val filePath = "src/test/resources/6-sample.vcf"

    val sd = new SequenceDictionary(Vector(SequenceRecord("6", 1999999L)))

    // load variant data
    val chr = "6"
    val viewRegion: ReferenceRegion = new ReferenceRegion(chr, 900000L, 999999L)
    val region: ReferenceRegion = new ReferenceRegion(chr, 900000L, 999999L)

    val vRDD: RDD[Genotype] = sc.loadGenotypes(filePath).filterByOverlappingRegion(viewRegion)
    val variantRDD: RDD[(ReferenceRegion, Genotype)] = vRDD.keyBy(v => new ReferenceRegion(v.variant.contig.contigName, v.variant.start,v.variant.end))

    var testRDD: IntervalRDD[ReferenceRegion,  Genotype] = IntervalRDD(variantRDD)
    val results = testRDD.get(viewRegion)
    assert(results.size == vRDD.count)

  }

  sparkTest("test count function") {

    val filePath = "src/test/resources/mouse_chrM.bam"
    val region = new ReferenceRegion("chrM", 0L, 1000L)

    val rdd: RDD[AlignmentRecord] = sc.loadIndexedBam(filePath, region)

    val alignmentRDD: RDD[(ReferenceRegion, AlignmentRecord)] = rdd.map(v => (region, v))

    var intRDD: IntervalRDD[ReferenceRegion,  AlignmentRecord] = IntervalRDD(alignmentRDD)


    assert(intRDD.count == rdd.count)

  }

  sparkTest("test filterByInterval") {

    val outRegion: ReferenceRegion = new ReferenceRegion(chr1, 200L, 299L)
    var intArr = Array((region1, rec1), (region1, rec2), (outRegion, rec3)) //region1 is 0-99
    var intArrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(intArr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(intArrRDD)

    val r: ReferenceRegion = new ReferenceRegion("chr1", 0L, 50L)
    val newRDD = testRDD.filterByInterval(r)
    assert(newRDD.count == 2)
    assert(testRDD.count == 3)
  }


  sparkTest("test explicit setting of number of partitions") {
    val filePath = "src/test/resources/mouse_chrM.bam"
    val region = new ReferenceRegion("chrM", 0L, 20000L)

    val rdd: RDD[AlignmentRecord] = sc.loadIndexedBam(filePath, region)

    val alignmentRDD: RDD[(ReferenceRegion, AlignmentRecord)] = rdd.map(v => (ReferenceRegion(v), v))

    var intRDD: IntervalRDD[ReferenceRegion,  AlignmentRecord] = IntervalRDD(alignmentRDD)

    assert(intRDD.count == rdd.count)

  }

  sparkTest("count data where chromosome partitions overlap") {
    var arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var arrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(arr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var intRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(arrRDD)
    assert(arrRDD.count == intRDD.count)
  }

  sparkTest("get data where chromosome partitions overlap") {
    var arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var arrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(arr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(arrRDD)

    val results1 = testRDD.get(region1)
    assert(results1.size == 1)

    val results2 = testRDD.get(region2)
    assert(results2.size == 1)

    val results3 = testRDD.get(region3)
    assert(results3.size == 1)

  }

  sparkTest("applying a predicate to the RDD") {

    val overlapReg: ReferenceRegion = new ReferenceRegion("chr1", 0L, 300L)

    var arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var arrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(arr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(arrRDD)
    val filtRDD = testRDD.filter(elem => elem._2 == "data1")
    val results = filtRDD.get(overlapReg)
    assert(results.size == 1)


  }

  sparkTest("testing collect") {

    var arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var arrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(arr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(arrRDD)
    val collected: Array[(ReferenceRegion, String)] = testRDD.collect()
    assert(collected.size == 3)

  }

  sparkTest("testing toRDD") {

    var arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var arrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(arr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(arrRDD)
    var backToRDD: RDD[(ReferenceRegion, String)] = testRDD.toRDD
    assert(backToRDD.collect.size == 3)

  }

  sparkTest("testing count") {

    var arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var arrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(arr)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(arrRDD)
    assert(testRDD.count == 3)

  }

  sparkTest("testing map") {

    var arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    var arrRDD: RDD[(ReferenceRegion, String)] = sc.parallelize(arr)
    val overlapReg: ReferenceRegion = new ReferenceRegion("chr1", 10L, 20L)

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 1000L),
      SequenceRecord("chr2", 1000L),
      SequenceRecord("chr3", 1000L)))

    var testRDD: IntervalRDD[ReferenceRegion,  String] = IntervalRDD(arrRDD)
    var mapRDD: IntervalRDD[ReferenceRegion, String] = testRDD.mapValues(elem => elem + "_mapped")
    val results = mapRDD.get(overlapReg)
    assert(results(0) == (region1, "data1_mapped"))
  }

  sparkTest("Attempt to access non existing data") {

    val filePath = "./mouse_chrM.bam"
    val region = new ReferenceRegion("chrM", 0L, 100L)
    val interval = new ReferenceRegion("X", 500L, 1000L)

    val rdd: RDD[AlignmentRecord] = sc.loadIndexedBam(filePath, region)

    val alignmentRDD: RDD[(ReferenceRegion, AlignmentRecord)] = rdd.map(v => (ReferenceRegion(v), v)).partitionBy(GenomicRegionPartitioner(10, sd))

    var intRDD: IntervalRDD[ReferenceRegion,  AlignmentRecord] = IntervalRDD(alignmentRDD)
    val results = intRDD.filterByInterval(interval)

    assert(results.count == 0)
  }


  sparkTest("Load and get alignment data from 2 different samples") {

    val sample1 = "HG000096"
    val sample2 = "HG000097"

    val records1 = new ListBuffer[AlignmentRecord]
    val records2 = new ListBuffer[AlignmentRecord]
    val sequence = "GATAAA"

    for (i <- 2L to 10L) {
      records1 += AlignmentRecord.newBuilder()
        .setStart(i)
        .setEnd(i + sequence.length)
        .setContigName("chr1")
        .setSequence(sequence)
        .setRecordGroupSample(sample1)
        .build
    }

    for (i <- 0L to 9L) {
      records2 += AlignmentRecord.newBuilder()
        .setStart(i)
        .setEnd(i + sequence.length)
        .setContigName("chr1")
        .setSequence(sequence)
        .setRecordGroupSample(sample2)
        .build
    }

    val data1 = sc.parallelize(records1).map(r => (ReferenceRegion(r), r))
    val data2 = sc.parallelize(records2).map(r => (ReferenceRegion(r), r))

    var intRDD: IntervalRDD[ReferenceRegion,  AlignmentRecord] = IntervalRDD(data1)

    intRDD = intRDD.multiput(data2)
    val inserted = intRDD.count

    val filtered = intRDD.filterByInterval(ReferenceRegion("chr1", 0L, 10L)).count
    assert(filtered == inserted)
  }
}
