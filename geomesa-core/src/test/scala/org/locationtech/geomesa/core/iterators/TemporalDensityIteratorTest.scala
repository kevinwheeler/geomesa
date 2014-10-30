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
import org.locationtech.geomesa.core.iterators.TemporalDensityIterator.{encodeTimeSeries, decodeTimeSeries, ENCODED_TIME_SERIES}
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.util.Random

import org.joda.time.{DateTime, DateTimeZone, Interval, Duration}


@RunWith(classOf[JUnitRunner])
class TemporalDensityIteratorTest extends Specification {

  sequential

  import org.locationtech.geomesa.utils.geotools.Conversions._

  def createDataStore(sft: SimpleFeatureType, i: Int = 0): DataStore = {
    val mockInstance = new MockInstance("dummy" + i)
    val c = mockInstance.getConnector("user", new PasswordToken("pass".getBytes))
    c.tableOperations.create("test")
    val splits = (0 to 99).map {
      s => "%02d".format(s)
    }.map(new Text(_))
    c.tableOperations().addSplits("test", new java.util.TreeSet[Text](splits))

    val dsf = new AccumuloDataStoreFactory

    import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._

    val ds = dsf.createDataStore(Map(
      zookeepersParam.key -> "dummy",
      instanceIdParam.key -> f"dummy$i%d",
      userParam.key       -> "user",
      passwordParam.key   -> "pass",
      tableNameParam.key  -> "test",
      mockParam.key       -> "true"))
    ds.createSchema(sft)
    ds
  }

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


  def getQuery(query: String): Query = {
    val q = new Query("test", ECQL.toFilter(query))
    val geom = q.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
    q.getHints.put(QueryHints.DENSITY_KEY, java.lang.Boolean.TRUE)
    q.getHints.put(QueryHints.BBOX_KEY, new ReferencedEnvelope(geom, DefaultGeographicCRS.WGS84))
    q.getHints.put(QueryHints.WIDTH_KEY, 500)
    q.getHints.put(QueryHints.HEIGHT_KEY, 500)
    q.getHints.put(QueryHints.TIME_INTERVAL_KEY, new Interval(new DateTime("2012-01-01T0:00:00", DateTimeZone.UTC).getMillis, new DateTime("2012-01-02T0:00:00", DateTimeZone.UTC).getMillis))
    q.getHints.put(QueryHints.TIME_BUCKETS_KEY, 24)
    q
  }

  "TemporalDensityIterator" should {
    val spec = "id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Geometry:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", spec)
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")
    val ds = createDataStore(sft,0)
    val encodedFeatures = (0 until 150).toArray.map{
      i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
    }
    val fs = loadFeatures(ds, sft, encodedFeatures)
    //  the iterator compresses the results into bins.
    //  there are less than 150 bin because they are all in the same point in time
    "reduce total features returned" in {
      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)") //time interval spans the whole datastore queried values
      val results = fs.getFeatures(q)
      val allFeatures = results.features()
      val iter = allFeatures.toList
      (iter must not).beNull

      iter.length should be lessThan 150
      iter.length should be equalTo 1 // one simpleFeature returned
    }

    //  checks that all the buckets' weights returned add up to 150
    "maintan total weights of time" in {

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")//time interval spans the whole datastore queried values

      val results = fs.getFeatures(q)
      val iter = results.features().toList
      val sf = iter.head.asInstanceOf[SimpleFeature]
      iter must not beNull

      val total = iter.map(_.getAttribute("weight").asInstanceOf[Double]).sum

      total should be equalTo 150
      val timeSeries = decodeTimeSeries(sf.getAttribute(ENCODED_TIME_SERIES).asInstanceOf[String])
      val totalCount = timeSeries.map { case (dateTime, count) => count}.sum

      totalCount should be equalTo 150
      timeSeries.size should be equalTo 1


    }

    // The points varied but that should have no effect on the bucketing of the timeseries
    //  Checks that all the buckets weight sum to 150, the original number of rows
    "maintain weight irrespective of point" in {
      val ds = createDataStore(sft, 1)
      val encodedFeatures = (0 until 150).toArray.map {
        i => Array(i.toString, "1.0", new DateTime("2012-01-01T19:00:00", DateTimeZone.UTC), s"POINT(-77.$i 38.$i)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")//time interval spans the whole datastore queried values

      val results = fs.getFeatures(q)
      val sfList = results.features().toList
      println(sfList)
      val sf = sfList.head.asInstanceOf[SimpleFeature]
      val timeSeries = decodeTimeSeries(sf.getAttribute(ENCODED_TIME_SERIES).asInstanceOf[String])

      val total = timeSeries.map { case (dateTime, count) => count}.sum

      total should be equalTo 150
      timeSeries.size should be equalTo 1
    }

    //this test actually varies the time therefore they should be split into seperate buckets
    //checks that there are 24 buckets, and that all the counts across the buckets sum to 150
    "correctly bin off of time intervals" in {
      val ds = createDataStore(sft, 2)
      val encodedFeatures = (0 until 48).toArray.map {
        i => Array(i.toString, "1.0", new DateTime(s"2012-01-01T${i%24}:00:00", DateTimeZone.UTC).toDate, "POINT(-77 38)")
      }
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")///ake the time interval 1 day and the number of buckets 24


      val results = fs.getFeatures(q) // returns one simple feature
      val sf = results.features().toList.head.asInstanceOf[SimpleFeature] //is this the time series??? im pretty sure it is
      val timeSeries = decodeTimeSeries(sf.getAttribute(ENCODED_TIME_SERIES).asInstanceOf[String])

      val total = timeSeries.map {
        case (dateTime, count) =>
          count should be equalTo 2L
          count}.sum

      total should be equalTo 48
      timeSeries.size should be equalTo 24
    }

    "encode decode feature" in {
      var timeSeries = new collection.mutable.HashMap[DateTime, Long]()
      timeSeries.put(new DateTime("2012-01-01T00:00:00", DateTimeZone.UTC), 2)
      timeSeries.put(new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC), 8)

      val encoded = TemporalDensityIterator.encodeTimeSeries(timeSeries)
      val decoded = TemporalDensityIterator.decodeTimeSeries(encoded)

      timeSeries should be equalTo decoded
      timeSeries.size should be equalTo 2
      timeSeries.get(new DateTime("2012-01-01T00:00:00", DateTimeZone.UTC)).get should be equalTo 2L
      timeSeries.get(new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC)).get should be equalTo 8L
    }

    "nothing to query over" in {
      val ds = createDataStore(sft, 3)
      val encodedFeatures = new Array[Array[_]](0)
      val fs = loadFeatures(ds, sft, encodedFeatures)

      val q = getQuery("(dtg between '2012-01-01T00:00:00.000Z' AND '2012-01-02T00:00:00.000Z') and BBOX(geom, -80, 33, -70, 40)")///ake the time interval 1 day and the number of buckets 24

      val results = fs.getFeatures(q) // returns one simple feature
      val sf = results.features().toList.head.asInstanceOf[SimpleFeature] //is this the time series??? im pretty sure it is
      sf must not be null


      val timeSeries = decodeTimeSeries(sf.getAttribute(ENCODED_TIME_SERIES).asInstanceOf[String])
      timeSeries.size should be equalTo 0
    }

    //more tests when we have the query hints working, these test will refine the getQuery calls
  }
}
