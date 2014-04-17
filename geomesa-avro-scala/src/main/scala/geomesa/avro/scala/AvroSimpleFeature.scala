package geomesa.avro.scala


import collection.JavaConversions._
import com.google.common.cache.{CacheLoader, LoadingCache, CacheBuilder}
import com.vividsolutions.jts.geom.Geometry
import java.io.OutputStream
import java.lang._
import java.nio._
import java.util
import java.util.concurrent.TimeUnit
import org.apache.avro.generic.{GenericDatumWriter, GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.{SchemaBuilder, Schema}
import org.geotools.data.DataUtilities
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.Converters
import org.opengis.feature.{Property, GeometryAttribute}
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}
import org.opengis.filter.identity.FeatureId
import java.util.{Map => JMap, Date, UUID, List => JList}
import org.opengis.geometry.BoundingBox
import org.opengis.feature.`type`.Name
import org.opengis.feature.`type`.AttributeDescriptor
import scala.util.Try


class AvroSimpleFeature(id: FeatureId, sft: SimpleFeatureType) extends SimpleFeature {

  import AvroSimpleFeature._

  val values    = Array.ofDim[AnyRef](sft.getAttributeCount)
  val userData  = collection.mutable.HashMap.empty[AnyRef, AnyRef]
  val typeMap   = typeMapCache.get(sft)
  val names     = nameCache.get(sft)
  val nameIndex = nameIndexCache.get(sft)
  val schema    = avroSchemaCache.get(sft)

  def write(os: OutputStream) {
    val encoder = EncoderFactory.get.binaryEncoder(os, null)
    val record = new GenericData.Record(schema)
    record.put(AvroSimpleFeature.AVRO_SIMPLE_FEATURE_VERSION, AvroSimpleFeature.VERSION)
    record.put(AvroSimpleFeature.FEATURE_ID_AVRO_FIELD_NAME, getID)

    values.zipWithIndex.foreach { case (v, idx) =>
      val x = typeMap(names(idx)) match {
        case t if primitiveTypes.contains(t) =>
          v

        case t if classOf[UUID].isAssignableFrom(t) =>
          val uuid = v.asInstanceOf[UUID]
          val bb = ByteBuffer.allocate(16)
          bb.putLong(uuid.getMostSignificantBits)
          bb.putLong(uuid.getLeastSignificantBits)
          bb.flip
          bb

        case t if classOf[Date].isAssignableFrom(t) =>
          v.asInstanceOf[Date].getTime

        case t if classOf[Geometry].isAssignableFrom(t) =>
          v.asInstanceOf[Geometry].toText

        case _ =>
          Option(Converters.convert(v, classOf[String])).getOrElse { a: AnyRef => a.toString }
      }

      record.put(names(idx), x)
    }
    val datumWriter = new GenericDatumWriter[GenericRecord](this.schema)
    datumWriter.write(record, encoder)
    encoder.flush()
  }

  def getFeatureType = sft
  def getType = sft
  def getIdentifier = id
  def getID = id.getID
  def getAttribute(name: String) = nameIndex.get(name).map(getAttribute).getOrElse(throw new NullPointerException("Invalid attribute"))
  def getAttribute(name: Name) = getAttribute(name.getLocalPart)
  def getAttribute(index: Int) = values(index)
  def setAttribute(name: String, value: Object) = setAttribute(nameIndex(name), value)
  def setAttribute(name: Name, value: Object) = setAttribute(name.getLocalPart, value)
  def setAttribute(index: Int, value: Object) = values(index) = value
  def setAttributes(values: JList[Object]) =
    values.zipWithIndex.foreach { case (v, idx) => setAttribute(idx, v) }

  def getAttributeCount = values.length
  def getAttributes: JList[Object] = values.toList
  def getDefaultGeometry: Object =
    Try(sft.getGeometryDescriptor.getName).map { getAttribute }.getOrElse(null)

  def setAttributes(`object`: Array[Object])= ???
  def setDefaultGeometry(defaultGeometry: Object) =
    setAttribute(sft.getGeometryDescriptor.getName, defaultGeometry)

  def getBounds: BoundingBox = getDefaultGeometry match {
    case g: Geometry =>
      new ReferencedEnvelope(g.getEnvelopeInternal, sft.getCoordinateReferenceSystem)
    case _ =>
      new ReferencedEnvelope(sft.getCoordinateReferenceSystem)
  }

  def getDefaultGeometryProperty: GeometryAttribute = ???
  def setDefaultGeometryProperty(defaultGeometry: GeometryAttribute) = ???
  def getProperties: util.Collection[Property] = ???
  def getProperties(name: Name): util.Collection[Property] = ???
  def getProperties(name: String): util.Collection[Property] = ???
  def getProperty(name: Name): Property = ???
  def getProperty(name: String): Property = ???
  def getValue: util.Collection[_ <: Property] = ???
  def setValue(value: util.Collection[Property]) = ???
  def getDescriptor: AttributeDescriptor = ???
  def getName: Name = ???
  def getUserData = userData
  def isNillable = true
  def setValue(value: Object) = ???
  def validate() = { }
}

object AvroSimpleFeature {
  import scala.collection.JavaConversions._

  val primitiveTypes =
    List(
      classOf[String],
      classOf[Integer],
      classOf[Long],
      classOf[Double],
      classOf[Float],
      classOf[Boolean]
    )

  def loadingCacheBuilder[V <: AnyRef](f: SimpleFeatureType => V) =
    CacheBuilder
      .newBuilder
      .maximumSize(100)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(
        new CacheLoader[SimpleFeatureType, V] {
          def load(sft: SimpleFeatureType): V = f(sft)
        }
      )

  val typeMapCache: LoadingCache[SimpleFeatureType, Map[String, Class[_]]] =
    loadingCacheBuilder { sft =>
      sft.getAttributeDescriptors.map { ad =>
        val name = ad.getLocalName
        val clazz = ad.getType.getBinding
        (name, clazz)
      }.toMap
    }

  val avroSchemaCache: LoadingCache[SimpleFeatureType, Schema] =
    loadingCacheBuilder { sft => generateSchema(sft) }

  val nameCache: LoadingCache[SimpleFeatureType, Array[String]] =
    loadingCacheBuilder { sft => DataUtilities.attributeNames(sft) }

  val nameIndexCache: LoadingCache[SimpleFeatureType, Map[String, Int]] =
    loadingCacheBuilder { sft =>
      DataUtilities.attributeNames(sft).map { name => (name, sft.indexOf(name)) }.toMap
    }


  final val FEATURE_ID_AVRO_FIELD_NAME: String = "__fid__"
  final val AVRO_SIMPLE_FEATURE_VERSION: String = "__version__"
  final val VERSION: Int = 1
  final val AVRO_NAMESPACE: String = "org.geomesa"

  def generateSchema(sft: SimpleFeatureType): Schema = {
    val initialAssembler: SchemaBuilder.FieldAssembler[Schema] =
      SchemaBuilder.record(sft.getTypeName)
        .namespace(AVRO_NAMESPACE)
        .fields
        .name(AVRO_SIMPLE_FEATURE_VERSION).`type`.intType.noDefault
        .name(FEATURE_ID_AVRO_FIELD_NAME).`type`.stringType.noDefault

    val result =
      sft.getAttributeDescriptors.foldLeft(initialAssembler) { case (assembler, ad) =>
        val name    = ad.getLocalName
        val binding = ad.getType.getBinding
        addField(assembler, name, binding)
      }

    result.endRecord
  }

  def addField(assembler: SchemaBuilder.FieldAssembler[Schema],
               name: String,
               ct: Class[_]): SchemaBuilder.FieldAssembler[Schema] =
    ct match {
      case c if classOf[String].isAssignableFrom(c)   => assembler.name(name).`type`.stringType.noDefault
      case c if classOf[Integer].isAssignableFrom(c)  => assembler.name(name).`type`.intType.noDefault
      case c if classOf[Long].isAssignableFrom(c)     => assembler.name(name).`type`.longType.noDefault
      case c if classOf[Double].isAssignableFrom(c)   => assembler.name(name).`type`.doubleType.noDefault
      case c if classOf[Float].isAssignableFrom(c)    => assembler.name(name).`type`.floatType.noDefault
      case c if classOf[Boolean].isAssignableFrom(c)  => assembler.name(name).`type`.booleanType.noDefault
      case c if classOf[UUID].isAssignableFrom(c)     => assembler.name(name).`type`.bytesType.noDefault
      case c if classOf[Date].isAssignableFrom(c)     => assembler.name(name).`type`.longType.noDefault
      case c if classOf[Geometry].isAssignableFrom(c) => assembler.name(name).`type`.stringType.noDefault
    }

}
