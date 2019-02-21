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

package com.spotify.featran

import org.apache.spark.rdd.{RDD, RDDUtil}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeTag}

package object spark {

  /**
   * [[CollectionType]] for extraction from Apache Spark `RDD` type.
   */
  implicit object SparkCollectionType extends CollectionType[RDD] {
    override def map[A, B: ClassTag](ma: RDD[A])(f: A => B): RDD[B] =
      ma.map(f)

    override def reduce[A](ma: RDD[A])(f: (A, A) => A): RDD[A] =
      ma.context.parallelize(Seq(ma.reduce(f)))(RDDUtil.classTag(ma))

    override def cross[A, B: ClassTag](ma: RDD[A])(mb: RDD[B]): RDD[(A, B)] = {
      val b = mb.first()
      ma.map((_, b))
    }

    override def pure[A, B: ClassTag](ma: RDD[A])(b: B): RDD[B] = ma.context.parallelize(Seq(b))
  }

  /**
    * [[CollectionType]] for extraction from Apache Spark `Dataset` type.
    */
  implicit object SparkDatasetType extends CollectionType[Dataset] {

    override def map[A, B: ClassTag](ma: Dataset[A])(f: A => B): Dataset[B] = {
      implicit lazy val ttB: TypeTag[B] = typeTag[B]
      mapOp(ma, f)
    }

    def mapOp[A, B: TypeTag](ma: Dataset[A], f: A => B): Dataset[B] = {
      implicit val encoderB: Encoder[B] = ExpressionEncoder[B]()
      ma.map(f)
    }

    override def reduce[A](ma: Dataset[A])(f: (A, A) => A): Dataset[A] = {
      implicit lazy val ttA: TypeTag[A] = typeTag[A]
      reduceOp(ma, f)
    }

    def reduceOp[A: TypeTag](ma: Dataset[A], f: (A, A) => A): Dataset[A] = {
      implicit val encoderA: Encoder[A] = ExpressionEncoder[A]()
      val reduced = ma.reduce(f)
      ma.sparkSession.createDataset(Seq(reduced))
    }

    override def cross[A, B: ClassTag](ma: Dataset[A])(mb: Dataset[B]): Dataset[(A, B)] = {
      implicit lazy val ttA: TypeTag[A] = typeTag[A]
      implicit lazy val ttB: TypeTag[B] = typeTag[B]
      crossOp(ma, mb)
    }

    def crossOp[A: TypeTag, B: TypeTag](ma: Dataset[A], mb: Dataset[B]): Dataset[(A, B)] = {
      implicit val encoderAB: Encoder[(A, B)] = ExpressionEncoder[(A, B)]()
      val b = mb.head()
      ma.map((_, b))
    }

    override def pure[A, B: ClassTag](ma: Dataset[A])(b: B): Dataset[B] = {
      implicit lazy val ttB: TypeTag[B] = typeTag[B]
      pureOp(ma, b)
    }

    def pureOp[A, B: TypeTag](ma: Dataset[A], b: B): Dataset[B] = {
      implicit val encoderB: Encoder[B] = ExpressionEncoder[B]()
      ma.sparkSession.createDataset(Seq(b))
    }
  }

}
