package org.locationtech.geomesa.core.process.temporaldensity

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.geotools.data.{Query, DataStoreFinder}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{Interval, DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data.{SimpleFeatureDecoder, AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.core.index.{QueryHints, Constants}
import org.locationtech.geomesa.core.iterators.TemporalDensityIterator.{decodeTimeSeries, ENCODED_TIME_SERIES}
import org.locationtech.geomesa.core.process.temporalDensity.TemporalDensityProcess
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TemporalDensityProcessTest extends Specification {

  sequential

  val dtgField = org.locationtech.geomesa.core.process.tube.DEFAULT_DTG_FIELD
  val geotimeAttributes = s"*geom:Geometry:srid=4326,$dtgField:Date"

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "mycloud",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "auths"             -> "A,B,C",
      "tableName"         -> "testwrite",
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

  val sftName = "geomesaTemporalDensityTestType"
  val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")
  sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

  val ds = createStore

  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val featureCollection = new DefaultFeatureCollection(sftName, sft)

  List("a", "b").foreach { name =>
    List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
      val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute(org.locationtech.geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:55Z", DateTimeZone.UTC).toDate)
      sf.setAttribute("type", name)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
  }

  // write the feature to the storeq
  val res = fs.addFeatures(featureCollection)

  def getQuery(interval: Interval): Query = {
    val dtf: DateTimeFormatter = ISODateTimeFormat.dateTime.withZone(DateTimeZone.UTC)
    val startTime = dtf.print(interval.getStartMillis)
    val endTime = dtf.print(interval.getStartMillis)
    val queryString = s"(dtg between '$startTime' AND '$endTime')"
    val q = new Query("test", ECQL.toFilter(queryString))
    q
  }

  "GeomesaTemporalDensity" should {
    "return stuffs" in {
      val interval = new Interval(0, new DateTime().getMillis, DateTimeZone.UTC)
      //val q = getQuery(interval)
      //val features = fs.getFeatures(q)
      val features = fs.getFeatures()

      val geomesaTDP = new TemporalDensityProcess
      val results = geomesaTDP.execute(features, interval, 4)
      //results.size mustEqual 1

      val f = results.toArray()(0).asInstanceOf[SimpleFeature]
      val timeSeries = decodeTimeSeries(f.getAttribute(ENCODED_TIME_SERIES).asInstanceOf[String])

      for ((key,value) <- timeSeries){
        println("key: " + key + " value: " + value)
      }
      results.size mustEqual 1
//
//
//      while (f.hasNext) {
//        val sf = f.next
//        sf.getAttribute("type") should beOneOf("a", "b")
//      }


    }
//
//    "respect a parent filter" in {
//      val features = fs.getFeatures(CQL.toFilter("type = 'b'"))
//
//      val geomesaQuery = new QueryProcess
//      val results = geomesaQuery.execute(features, null)
//
//      val f = results.features()
//      while (f.hasNext) {
//        val sf = f.next
//        sf.getAttribute("type") mustEqual "b"
//      }
//
//      results.size mustEqual 4
//    }
//
//    "be able to use its own filter" in {
//      val features = fs.getFeatures(CQL.toFilter("type = 'b' OR type = 'a'"))
//
//      val geomesaQuery = new QueryProcess
//      val results = geomesaQuery.execute(features, CQL.toFilter("type = 'a'"))
//
//      val f = results.features()
//      while (f.hasNext) {
//        val sf = f.next
//        sf.getAttribute("type") mustEqual "a"
//      }
//
//      results.size mustEqual 4
//    }
//
//    "properly query geometry" in {
//      val features = fs.getFeatures()
//
//      val geomesaQuery = new QueryProcess
//      val results = geomesaQuery.execute(features, CQL.toFilter("bbox(geom, 45.0, 45.0, 46.0, 46.0)"))
//
//      var poly = WKTUtils.read("POLYGON((45 45, 46 45, 46 46, 45 46, 45 45))")
//
//      val f = results.features()
//      while (f.hasNext) {
//        val sf = f.next
//        poly.intersects(sf.getDefaultGeometry.asInstanceOf[Geometry]) must beTrue
//      }
//
//      results.size mustEqual 4
//    }
  }


}
