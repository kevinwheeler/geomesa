package org.locationtech.geomesa.core.process.temporalDensity

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult, FeatureCalc}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.util.NullProgressListener
import org.joda.time.{DateTime, Interval}
import org.locationtech.geomesa.core.index.QueryHints
import org.locationtech.geomesa.core.iterators.TemporalDensityIterator
import org.locationtech.geomesa.core.iterators.TemporalDensityIterator._
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature

import scala.util.Random
import scala.util.parsing.json.JSONObject

@DescribeProcess(
  title = "Temporal Density Query",
  description = "Determines the number of query results at different time buckets within an interval"
)
class TemporalDensityProcess extends Logging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "The feature set on which to query")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "startDate",
                 description = "The start of the time interval over which we want result density information")
               startDate: Date,
               @DescribeParameter(
                 name = "endDate",
                 description = "The end of the time interval over which we want result density information")
               endDate: Date,
               @DescribeParameter(
                 name = "buckets",
                 min = 1,
                 description = "How many buckets we want to divide our time interval into.")
               buckets: Int
               ): SimpleFeatureCollection = {

    logger.info("Attempting Geomesa temporal density on type " + features.getClass.getName)

    if(features.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

   val interval = new Interval(startDate.getTime, endDate.getTime)

    val visitor = new TemporalDensityVisitor(features, interval, buckets)
    features.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[TDResult].results
  }
}

class TemporalDensityVisitor(features: SimpleFeatureCollection, interval: Interval, buckets: Int )
  extends FeatureCalc
          with Logging {

  // JNH: Schema should be the schema from the TDI.
  val manualVisitResults = new DefaultFeatureCollection(null, features.getSchema)
 // val ff  = CommonFactoryFinder.getFilterFactory2

 //  Called for non AccumuloFeatureCollections
   def visit(feature: Feature): Unit = {
     val sf = feature.asInstanceOf[SimpleFeature]
       manualVisitResults.add(sf)
  }

  var resultCalc: TDResult = new TDResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) {
    val sfList = r.features().toList
//      val TIME_SERIES_JSON: String = "timeseriesjson"
//      val TEMPORAL_DENSITY_FEATURE_STRING = s"$TIME_SERIES_JSON:String,geom:Geometry"
      val projectedSFT = SimpleFeatureTypes.createType(this.getClass.getCanonicalName, TEMPORAL_DENSITY_FEATURE_STRING)
      val retCollection = new DefaultFeatureCollection(null, projectedSFT)
    if (sfList.length == 0) {
      resultCalc = TDResult(retCollection)
    }
    else {
      val sf = sfList(0)
      val timeSeries = decodeTimeSeries(sf.getAttribute(ENCODED_TIME_SERIES).asInstanceOf[String])
      val s = timeSeries.toMap map { case (k, v) => (k.toString(null) -> v)}
      val json = new JSONObject(s).toString()
      val featureBuilder = AvroSimpleFeatureFactory.featureBuilder(projectedSFT)
      featureBuilder.reset()
      featureBuilder.add(json)
      featureBuilder.add(TemporalDensityIterator.zeroPoint) //Filler value as Feature requires a geometry
      val feature = featureBuilder.buildFeature(Random.nextString(6))
      retCollection.add(feature)
      resultCalc = TDResult(retCollection)
    }
  }

//  def setValue(r: SimpleFeatureCollection) = resultCalc = TDResult(r)

  def query(source: SimpleFeatureSource, query: Query) = {
    logger.info("Running Geomesa query on source type "+source.getClass.getName)
    query.getHints.put(QueryHints.TEMPORAL_DENSITY_KEY, java.lang.Boolean.TRUE)
    query.getHints.put(QueryHints.TIME_INTERVAL_KEY, interval)
    query.getHints.put(QueryHints.TIME_BUCKETS_KEY, buckets)
    source.getFeatures(query)
  }
}

case class TDResult(results: SimpleFeatureCollection) extends AbstractCalcResult