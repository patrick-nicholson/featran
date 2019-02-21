/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.featran.spark

import com.spotify.featran._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest._

class SparkTest extends FlatSpec with Matchers {

  import Fixtures._

  "FeatureSpec" should "work with Spark RDD" in {
    val sc = new SparkContext("local[4]", "test")
    val f = TestSpec.extract(sc.parallelize(TestData))
    f.featureNames.collect() shouldBe Array(ExpectedNames)
    f.featureValues[Seq[Double]].collect() should contain theSameElementsAs ExpectedValues
    f.featureValues[Map[String, Double]]
      .collect() should contain theSameElementsAs ExpectedMapValues
    sc.stop()
  }

  "MultiFeatureSpec" should "work with Spark RDD" in {
    noException shouldBe thrownBy {
      val sc = new SparkContext("local[4]", "test")
      val f = RecordSpec.extract(sc.parallelize(Records))
      f.featureNames.collect()
      f.featureValues[Seq[Double]].collect()
      sc.stop()
    }
  }

  "FeatureSpec" should "work with Spark Dataset" in {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("test")
      .getOrCreate()
    import spark.implicits._
    val f = TestSpec.extract(spark.createDataset(TestData))
    f.featureNames.collect() shouldBe Array(ExpectedNames)
    f.featureValues[Seq[Double]].collect() should contain theSameElementsAs ExpectedValues
    f.featureValues[Map[String, Double]]
      .collect() should contain theSameElementsAs ExpectedMapValues
    spark.stop()
  }

  "MultiFeatureSpec" should "work with Spark Dataset" in {
    noException shouldBe thrownBy {
      val spark = SparkSession.builder()
        .master("local[4]")
        .appName("test")
        .getOrCreate()
      import spark.implicits._
      val f = RecordSpec.extract(spark.createDataset(Records))
      f.featureNames.collect()
      f.featureValues[Seq[Double]].collect()
      spark.stop()
    }
  }

}
