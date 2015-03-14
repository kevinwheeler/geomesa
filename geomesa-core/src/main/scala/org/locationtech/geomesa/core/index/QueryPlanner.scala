/*
 * Copyright 2014-2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index

import java.util.Map.Entry
import java.util.{Map => JMap}

import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.iterators.TemporalDensityIterator._
import org.locationtech.geomesa.core.iterators.{DeDuplicatingIterator, DensityIterator, MapAggregatingIterator, TemporalDensityIterator}
import org.locationtech.geomesa.core.security.SecurityUtils
import org.locationtech.geomesa.core.sumNumericValueMutableMaps
import org.locationtech.geomesa.core.util.CloseableIterator._
import org.locationtech.geomesa.core.util.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.{ScalaSimpleFeatureFactory, SimpleFeatureDecoder}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.sort.{SortBy, SortOrder}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object QueryPlanner {
  val iteratorPriority_RowRegex                        = 0
  val iteratorPriority_AttributeIndexFilteringIterator = 10
  val iteratorPriority_AttributeIndexIterator          = 200
  val iteratorPriority_AttributeUniqueIterator         = 300
  val iteratorPriority_ColFRegex                       = 100
  val iteratorPriority_SpatioTemporalIterator          = 200
  val iteratorPriority_SimpleFeatureFilteringIterator  = 300
  val iteratorPriority_AnalysisIterator                = 400

  val zeroPoint = new GeometryFactory().createPoint(new Coordinate(0,0))
}

case class QueryPlanner(schema: String,
                        featureType: SimpleFeatureType,
                        featureEncoding: FeatureEncoding) extends ExplainingLogging with IndexFilterHelpers {

  // As a pre-processing step, we examine the query/filter and split it into multiple queries.
  // TODO: Work to make the queries non-overlapping.
  def getIterator(acc: AccumuloConnectorCreator,
                  sft: SimpleFeatureType,
                  query: Query,
                  hints: StrategyHints,
                  output: ExplainerOutputType = log): CloseableIterator[Entry[Key,Value]] = {

    output(s"Running ${ExplainerOutputType.toString(query)}")
    val ff = CommonFactoryFinder.getFilterFactory2
    val isDensity = query.getHints.containsKey(BBOX_KEY)
    val duplicatableData = IndexSchema.mayContainDuplicates(featureType)

    def flatten(queries: Seq[Query]): CloseableIterator[Entry[Key, Value]] =
      queries.toIterator.ciFlatMap(configureScanners(acc, sft, _, hints, isDensity, output))

    // in some cases, where duplicates may appear in overlapping queries or the data itself, remove them
    def deduplicate(queries: Seq[Query]): CloseableIterator[Entry[Key, Value]] = {
      val flatQueries = flatten(queries)
      val decoder = SimpleFeatureDecoder(getReturnSFT(query), featureEncoding)
      new DeDuplicatingIterator(flatQueries, (key: Key, value: Value) => decoder.extractFeatureId(value.get))
    }

    if(isDensity) {
      val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
      val q1 = new Query(featureType.getTypeName, ff.bbox(ff.property(featureType.getGeometryDescriptor.getLocalName), env))
      val mixedQuery = DataUtilities.mixQueries(q1, query, "geomesa.mixed.query")
      if (duplicatableData) {
        deduplicate(Seq(mixedQuery))
      } else {
        flatten(Seq(mixedQuery))
      }
    } else {
      val rawQueries = splitQueryOnOrs(query, output)
      if (rawQueries.length > 1 || duplicatableData) {
        deduplicate(rawQueries)
      } else {
        flatten(rawQueries)
      }
    }
  }
  
  def splitQueryOnOrs(query: Query, output: ExplainerOutputType): Seq[Query] = {
    val originalFilter = query.getFilter
    output(s"Original filter: $originalFilter")

    val rewrittenFilter = rewriteFilterInDNF(originalFilter)
    output(s"Rewritten filter: $rewrittenFilter")

    val orSplitter = new OrSplittingFilter
    val splitFilters = orSplitter.visit(rewrittenFilter, null)

    // Let's just check quickly to see if we can eliminate any duplicates.
    val filters = splitFilters.distinct

    filters.map { filter =>
      val q = new Query(query)
      q.setFilter(filter)
      q
    }
  }

  /**
   * Helper method to execute a query against an AccumuloDataStore
   *
   * If the query contains ONLY an eligible LIKE
   * or EQUALTO query then satisfy the query with the attribute index
   * table...else use the spatio-temporal index table
   *
   * If the query is a density query use the spatio-temporal index table only
   */
  private def configureScanners(acc: AccumuloConnectorCreator,
                       sft: SimpleFeatureType,
                       derivedQuery: Query,
                       hints: StrategyHints,
                       isADensity: Boolean,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    output(s"Transforms: ${derivedQuery.getHints.get(TRANSFORMS)}")
    val strategy = QueryStrategyDecider.chooseStrategy(sft, derivedQuery, hints, acc.getGeomesaVersion(sft))

    output(s"Strategy: ${strategy.getClass.getCanonicalName}")
    strategy.execute(acc, this, sft, derivedQuery, output)
  }

  def query(query: Query, acc: AccumuloConnectorCreator, hints: StrategyHints):
      CloseableIterator[SimpleFeature] = {
    // Perform the query
    val accumuloIterator = getIterator(acc, featureType, query, hints)

    // Convert Accumulo results to SimpleFeatures
    adaptIterator(accumuloIterator, query)
  }

  // This function decodes/transforms that Iterator of Accumulo Key-Values into an Iterator of SimpleFeatures.
  def adaptIterator(accumuloIterator: CloseableIterator[Entry[Key,Value]], query: Query): CloseableIterator[SimpleFeature] = {
    // Perform a projecting decode of the simple feature
    val returnSFT = getReturnSFT(query)
    val decoder = SimpleFeatureDecoder(returnSFT, featureEncoding)

    // Decode according to the SFT return type.
    // if this is a density query, expand the map
    if (query.getHints.containsKey(DENSITY_KEY)) {
      adaptIteratorForDensityQuery(accumuloIterator, decoder)
    } else if (query.getHints.containsKey(TEMPORAL_DENSITY_KEY)) {
      adaptIteratorForTemporalDensityQuery(accumuloIterator, returnSFT, decoder, query.getHints.containsKey(RETURN_ENCODED))
    } else if (query.getHints.containsKey(MAP_AGGREGATION_KEY)) {
      adaptIteratorForMapAggregationQuery(accumuloIterator, query, returnSFT, decoder)
    } else {
      adaptIteratorForStandardQuery(accumuloIterator, query, decoder)
    }
  }


  def adaptIteratorForStandardQuery(accumuloIterator: CloseableIterator[Entry[Key, Value]],
                                    query: Query,
                                    decoder: SimpleFeatureDecoder): CloseableIterator[SimpleFeature] = {
    val features = accumuloIterator.map { kv =>
      val ret = decoder.decode(kv.getValue.get)
      val visibility = kv.getKey.getColumnVisibility
      if(visibility != null && !EMPTY_VIZ.equals(visibility)) {
        ret.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, visibility.toString)
      }
      ret
    }

    if (query.getSortBy != null && query.getSortBy.length > 0) sort(features, query.getSortBy)
    else features
  }

  def adaptIteratorForTemporalDensityQuery(accumuloIterator: CloseableIterator[Entry[Key, Value]],
                                           returnSFT: SimpleFeatureType,
                                           decoder: SimpleFeatureDecoder,
                                           returnEncoded: Boolean): CloseableIterator[SimpleFeature] = {
    val timeSeriesStrings = accumuloIterator.map { kv =>
      decoder.decode(kv.getValue.get).getAttribute(TIME_SERIES).toString
    }
    val summedTimeSeries = timeSeriesStrings.map(decodeTimeSeries).reduce(combineTimeSeries)

    val featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(returnSFT)
    featureBuilder.reset()

    if (returnEncoded) {
      featureBuilder.add(TemporalDensityIterator.encodeTimeSeries(summedTimeSeries))
    } else {
      featureBuilder.add(timeSeriesToJSON(summedTimeSeries))
    }


    featureBuilder.add(QueryPlanner.zeroPoint) //Filler value as Feature requires a geometry
    val result = featureBuilder.buildFeature(null)

    List(result).iterator
  }

  def adaptIteratorForDensityQuery(accumuloIterator: CloseableIterator[Entry[Key, Value]],
                                   decoder: SimpleFeatureDecoder): CloseableIterator[SimpleFeature] = {
    accumuloIterator.flatMap { kv =>
      DensityIterator.expandFeature(decoder.decode(kv.getValue.get))
    }
  }

  def adaptIteratorForMapAggregationQuery(accumuloIterator: CloseableIterator[Entry[Key, Value]],
                                          query: Query,
                                          returnSFT: SimpleFeatureType,
                                          decoder: SimpleFeatureDecoder): CloseableIterator[SimpleFeature] = {
    val aggregateKeyName = query.getHints.get(MAP_AGGREGATION_KEY).asInstanceOf[String]

    val maps = accumuloIterator.map { kv =>
      decoder
        .decode(kv.getValue.get)
        .getAttribute(aggregateKeyName)
        .asInstanceOf[JMap[AnyRef, Int]]
        .asScala
    }

    if(maps.nonEmpty) {
      val reducedMap = sumNumericValueMutableMaps(maps.toIterable).toMap // to immutable map

      val featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(returnSFT)
      featureBuilder.reset()
      featureBuilder.add(reducedMap)
      featureBuilder.add(QueryPlanner.zeroPoint) //Filler value as Feature requires a geometry
      val result = featureBuilder.buildFeature(null)

      Iterator(result)
    } else Iterator()
  }

  private def sort(features: CloseableIterator[SimpleFeature],
                   sortBy: Array[SortBy]): CloseableIterator[SimpleFeature] = {
    val sortOrdering = sortBy.map {
      case SortBy.NATURAL_ORDER => Ordering.by[SimpleFeature, String](_.getID)
      case SortBy.REVERSE_ORDER => Ordering.by[SimpleFeature, String](_.getID).reverse
      case sb                   =>
        val prop = sb.getPropertyName.getPropertyName
        val ord  = attributeToComparable(prop)
        if(sb.getSortOrder == SortOrder.DESCENDING) ord.reverse
        else ord
    }
    val comp: (SimpleFeature, SimpleFeature) => Boolean =
      if(sortOrdering.length == 1) {
        // optimized case for one ordering
        val ret = sortOrdering.head
        (l, r) => ret.compare(l, r) < 0
      }  else {
        (l, r) => sortOrdering.map(_.compare(l, r)).find(_ != 0).getOrElse(0) < 0
      }
    CloseableIterator(features.toList.sortWith(comp).iterator)
  }

  def attributeToComparable[T <: Comparable[T]](prop: String)(implicit ct: ClassTag[T]): Ordering[SimpleFeature] =
      Ordering.by[SimpleFeature, T](_.getAttribute(prop).asInstanceOf[T])

  // This function calculates the SimpleFeatureType of the returned SFs.
  private def getReturnSFT(query: Query): SimpleFeatureType =
    query match {
      case _: Query if query.getHints.containsKey(DENSITY_KEY)  =>
        SimpleFeatureTypes.createType(featureType.getTypeName, DensityIterator.DENSITY_FEATURE_SFT_STRING)
      case _: Query if query.getHints.containsKey(TEMPORAL_DENSITY_KEY)  =>
        createFeatureType(featureType)
      case _: Query if query.getHints.containsKey(MAP_AGGREGATION_KEY)  =>
        val mapAggregationAttribute = query.getHints.get(MAP_AGGREGATION_KEY).asInstanceOf[String]
        val sftSpec = MapAggregatingIterator.projectedSFTDef(mapAggregationAttribute, featureType)
        SimpleFeatureTypes.createType(featureType.getTypeName, sftSpec)
      case _: Query if query.getHints.get(TRANSFORM_SCHEMA) != null =>
        query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType]
      case _ => featureType
    }
}
