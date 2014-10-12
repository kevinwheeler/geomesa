package org.locationtech.geomesa.core.process.query

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{AbstractCalcResult, CalcResult, FeatureCalc}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.util.NullProgressListener
import org.joda.time.Interval
import org.locationtech.geomesa.core.index.QueryHints
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

@DescribeProcess(
  title = "Temporal Density Query",
  description = "Determines the number of query results at different time bins"
)
class TemporalDensityProcess extends Logging {

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "The feature set on which to query")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "filter",
                 min = 0,
                 description = "The filter to apply to the features collection")
               filter: Filter,
               @DescribeParameter(
                 name = "interval",
                 description = "The time interval over which we want result density information")
               interval: Interval,
               @DescribeParameter(
                 name = "numBins",
                 min = 1,
                 description = "How many bins we want to divide our time interval into.")
               numBins: Int
               ): SimpleFeatureCollection = {

    logger.info("Attempting Geomesa query on type " + features.getClass.getName)

    if(features.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val visitor = new TemporalDensityVisitor(features, interval, numBins)
    features.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[QueryResult].results
  }
}

class TemporalDensityVisitor(features: SimpleFeatureCollection, interval: Interval, numBins: Int )
  extends FeatureCalc
          with Logging {

  // JNH: Schema should be the schema from the TDI.
  val manualVisitResults = new DefaultFeatureCollection(null, features.getSchema)
 // val ff  = CommonFactoryFinder.getFilterFactory2

 //  Called for non AccumuloFeactureCollections
   def visit(feature: Feature): Unit = {
     val sf = feature.asInstanceOf[SimpleFeature]
       manualVisitResults.add(sf)
  }

  var resultCalc: QueryResult = new QueryResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = QueryResult(r)

  def query(source: SimpleFeatureSource, query: Query) = {
    logger.info("Running Geomesa query on source type "+source.getClass.getName)
    query.getHints.put(QueryHints.TEMPORAL_DENSITY_KEY, java.lang.Boolean.TRUE)
    query.getHints.put(QueryHints.INTERVAL_KEY, interval)
    query.getHints.put(QueryHints.NUM_BINS_KEY, numBins)
    source.getFeatures(query)
  }
}

//case class QueryResult(results: SimpleFeatureCollection) extends AbstractCalcResult