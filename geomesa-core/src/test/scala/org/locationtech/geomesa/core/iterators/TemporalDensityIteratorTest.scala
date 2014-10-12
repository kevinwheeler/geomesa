/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.locationtech.geomesa.core.iterators

import java.util.Date

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.security.Authorizations
import com.google.common.collect.HashBasedTable
import com.vividsolutions.jts.geom.{Envelope, Point}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.io.Text
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataUtilities, Query}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index.{Constants, QueryHints}
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

import org.joda.time.{DateTime, DateTimeZone, Interval, Duration}


@RunWith(classOf[JUnitRunner])
class TemporalDensityIteratorTest extends Specification {

  sequential

  import org.locationtech.geomesa.utils.geotools.Conversions._

  def loadFeatures(ds: DataStore, sft: SimpleFeatureType, encodedFeatures: Array[_ <: Array[_]]): SimpleFeatureStore = {
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    val features = encodedFeatures.map {
      e =>
        val f = builder.buildFeature(e(0).toString, e.asInstanceOf[Array[AnyRef]])
        f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        f.getUserData.put(Hints.PROVIDED_FID, e(0).toString)
        f
    }

    val fs = ds.getFeatureSource("test").asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(DataUtilities.collection(features))
    fs.getTransaction.commit()
    fs
  }


  "TemporalDensityIterator" should {

    // This block creates the SimpleFeatures to play with

    // TODO: Something useful:
    //  Make times 'more' unique
    val spec = "id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Geometry:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", spec)
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)

    import org.locationtech.geomesa.core.data.SimpleFeatureEncoder
    val encoder = SimpleFeatureEncoder(sft, FeatureEncoding.AVRO)

    val returnSFT = SimpleFeatureTypes.createType("ret", TemporalDensityIterator.TEMPORAL_DENSITY_FEATURE_STRING)
    val decoder = SimpleFeatureDecoder(returnSFT, FeatureEncoding.AVRO)

    sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")
    //val ds = createDataStore(sft, 0)
    val encodedFeatures = (0 until 150).toArray.map {
      i =>
        Array(i.toString, "1.0", new DateTime(s"2012-01-01T${i % 24}:${(i*13)%60}:00-00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
    }

    val table = "TDITestTable"
    val instance = new MockInstance(table)
    val conn = instance.getConnector("", new PasswordToken(""))
    conn.tableOperations.create(table, true, TimeType.LOGICAL)

    val bw = conn.createBatchWriter(table, new BatchWriterConfig)

    val features = encodedFeatures.map {
      e =>
        val f = builder.buildFeature(e(0).toString, e.asInstanceOf[Array[AnyRef]])
        f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        f.getUserData.put(Hints.PROVIDED_FID, e(0).toString)
        f
    }

    features.foreach { f =>
      val rand = Random.nextString(5)
      val mut = new Mutation("key" + rand)

      val encodedFeature = encoder.encode(f)

      mut.put(new Text(""), new Text(""), new Value(encodedFeature))
      // Add encoded value = encodedFeature 
      bw.addMutation(mut)
    }
    bw.close()


    "Bin all the data sanely" in {
      val scanner = conn.createScanner(table, new Authorizations())

      // Configure TDI on the scanner
      val is = new IteratorSetting(40, classOf[TemporalDensityIterator])
      TemporalDensityIterator.configure(is, new Interval(new DateTime("2012-01-01T0:00:00").getMillis, new DateTime("2012-01-01T23:59:59").getMillis), 1)

      is.addOption(FEATURE_ENCODING, FeatureEncoding.AVRO.toString)
      val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(sft)
      is.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
      is.encodeUserData(sft.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

      scanner.addScanIterator(is)
      //Set Iterator Range?

      // Execute the scan, we will get 1 result which is the encoded Timeseries
      //  Result size must be 1
      val results = scanner.iterator.toList //list of entry of k:v encoded
      results.size should be equalTo 1 // one TimeSeries
      // The time series decoded must have one (non-empty) bucket
      val decoded = results.map { r =>
          val encodedValue: Value = r.getValue // Array[Bytes]?

          val decodedValue = decoder.decode(encodedValue) // TODO
        val encodedTS = decodedValue.getAttribute(TemporalDensityIterator.ENCODED_TIME_SERIES).asInstanceOf[String]

          TemporalDensityIterator.decodeTimeSeries(encodedTS)
        }

      println(s"Hashmap: ${decoded(0)}")

      decoded(0).values.head should be equalTo 150
      // TODO: Add
      decoded.size should be equalTo 1

    }

    "Bin data in multiple buckets" in {
      val scanner = conn.createScanner(table, new Authorizations())

      // Configure TDI on the scanner
      val is = new IteratorSetting(40, classOf[TemporalDensityIterator])
      TemporalDensityIterator.configure(is, new Interval(new DateTime("2012-01-01T0:00:00-00:00").getMillis, new DateTime("2012-01-02T0:00:00-00:00").getMillis), 24)

      is.addOption(FEATURE_ENCODING, FeatureEncoding.AVRO.toString)
      val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(sft)
      is.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
      is.encodeUserData(sft.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

      scanner.addScanIterator(is)

      // Execute the scan, we will get 1 result which is the encoded Timeseries
      //  Result size must be 1
      val results = scanner.iterator.toList //list of entry of k:v encoded
      results.size should be equalTo 1 // one TimeSeries
      // The time series decoded must have one (non-empty) bucket
      val decoded = results.map { r =>
          val encodedValue: Value = r.getValue // Array[Bytes]?

          val decodedValue = decoder.decode(encodedValue) // TODO
        val encodedTS = decodedValue.getAttribute(TemporalDensityIterator.ENCODED_TIME_SERIES).asInstanceOf[String]

          TemporalDensityIterator.decodeTimeSeries(encodedTS)
        }

      //there might be a TimeSnap error bucketing correctly due to timezone being left off
      //might be because of DateTime's default timeZone offset
      //turn off computer defualt timezone offset

      for ((k, v) <- decoded(0)) {
        //println(s"$k , $v")
        v should be greaterThan 5
      }
      decoded(0).size should be equalTo 24
    }

    "Bin data into strange bucket sizes" in {
      val scanner = conn.createScanner(table, new Authorizations())

      // Configure TDI on the scanner
      val is = new IteratorSetting(40, classOf[TemporalDensityIterator])
      TemporalDensityIterator.configure(is, new Interval(new DateTime("2012-01-01T0:00:00-00:00").getMillis, new DateTime("2012-01-02T0:00:00-00:00").getMillis), 48)

      is.addOption(FEATURE_ENCODING, FeatureEncoding.AVRO.toString)
      val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(sft)
      is.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
      is.encodeUserData(sft.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

      scanner.addScanIterator(is)


      // Execute the scan, we will get 1 result which is the encoded Timeseries
      //  Result size must be 1
      val results = scanner.iterator.toList //list of entry of k:v encoded
      results.size should be equalTo 1 // one TimeSeries
      // The time series decoded must have one (non-empty) bucket
      val decoded = results.map { r =>
          val encodedValue: Value = r.getValue // Array[Bytes]?

          val decodedValue = decoder.decode(encodedValue) // TODO
        val encodedTS = decodedValue.getAttribute(TemporalDensityIterator.ENCODED_TIME_SERIES).asInstanceOf[String]

          TemporalDensityIterator.decodeTimeSeries(encodedTS)
        }


      for ((k, v) <- decoded(0)) {
        println(s"$k , $v")
        k.getMinuteOfHour()%30 should be equalTo 0
      }
      decoded(0).size should be equalTo 48
    }
  }
}
