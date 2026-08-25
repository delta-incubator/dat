/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.workload.json

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, DeserializationFeature, JsonDeserializer, JsonNode, JsonSerializer, ObjectMapper, SerializationFeature, SerializerProvider}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types.{DataType, StructType}

import io.delta.workload.model.{Failed, ReadQuery, ReadResult, ReadSpec, SnapshotQuery, SnapshotResult, SnapshotSpec, Spec, SpecError, SpecExpectation, Succeeded}

/**
 * Flattens the outcome to top-level `expected` (success) XOR `error` (failure). `@JsonUnwrapped`
 * can't populate a case-class constructor param (Jackson rejects it as a creator property), so the
 * thin per-spec serdes below delegate the flatten to this.
 */
private object SpecExpectationJson {
  def write[R](g: JsonGenerator, e: SpecExpectation[R]): Unit = e match {
    case Succeeded(result) => g.writeObjectField("expected", result)
    case Failed(error) => g.writeObjectField("error", error)
  }

  /**
   * Write the shared spec envelope: `type`, optional `version`/`timestamp`, then any spec-specific
   * `extra` fields, then the `expected`/`error` outcome.
   */
  def writeEnvelope[R](g: JsonGenerator, tpe: String, version: Option[Long],
      timestamp: Option[String], expectation: SpecExpectation[R])(extra: => Unit): Unit = {
    g.writeStartObject()
    g.writeStringField("type", tpe)
    version.foreach(v => g.writeNumberField("version", v))
    timestamp.foreach(t => g.writeStringField("timestamp", t))
    extra
    write(g, expectation)
    g.writeEndObject()
  }
  def read[R](node: JsonNode, resultClass: Class[R]): SpecExpectation[R] =
    Option(node.get("error")).filterNot(_.isNull) match {
      case Some(err) => Failed(JsonUtil.mapper.treeToValue(err, classOf[SpecError]))
      case None =>
        val expected = Option(node.get("expected")).filterNot(_.isNull).getOrElse(
          throw new IllegalArgumentException("spec declares neither `expected` nor `error`"))
        Succeeded(JsonUtil.mapper.treeToValue(expected, resultClass))
    }
  def optLong(n: JsonNode, f: String): Option[Long] = Option(n.get(f)).filterNot(_.isNull).map(_.asLong)
  def optText(n: JsonNode, f: String): Option[String] = Option(n.get(f)).filterNot(_.isNull).map(_.asText)
  def optStrings(n: JsonNode, f: String): Option[Seq[String]] =
    Option(n.get(f)).filterNot(_.isNull).map(_.elements().asScala.map(_.asText).toSeq)
}

// Spec serializers write `type` themselves (the EXISTING_PROPERTY discriminator), so
// serializeWithType needs no type-id wrapper and just delegates to serialize.
private[workload] abstract class SpecSerializer[S] extends JsonSerializer[S] {
  override def serializeWithType(s: S, g: JsonGenerator, p: SerializerProvider,
      typeSer: com.fasterxml.jackson.databind.jsontype.TypeSerializer): Unit = serialize(s, g, p)
}
// Spec deserializers all read the whole node first, then build from it.
private[workload] abstract class SpecDeserializer[S] extends JsonDeserializer[S] {
  protected def fromNode(n: JsonNode): S
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): S =
    fromNode(jp.getCodec.readTree[JsonNode](jp))
}

private[workload] class ReadSpecSerializer extends SpecSerializer[ReadSpec] {
  override def serialize(s: ReadSpec, g: JsonGenerator, p: SerializerProvider): Unit =
    SpecExpectationJson.writeEnvelope(g, s.`type`, s.query.version, s.query.timestamp, s.expectation) {
      s.query.predicate.foreach(pr => g.writeStringField("predicate", pr))
      s.query.columns.foreach { cols =>
        g.writeArrayFieldStart("columns"); cols.foreach(g.writeString); g.writeEndArray()
      }
    }
}
private[workload] class ReadSpecDeserializer extends SpecDeserializer[ReadSpec] {
  override protected def fromNode(n: JsonNode): ReadSpec =
    ReadSpec(
      ReadQuery(SpecExpectationJson.optLong(n, "version"), SpecExpectationJson.optText(n, "timestamp"),
        SpecExpectationJson.optText(n, "predicate"), SpecExpectationJson.optStrings(n, "columns")),
      SpecExpectationJson.read(n, classOf[ReadResult]))
}
private[workload] class SnapshotSpecSerializer extends SpecSerializer[SnapshotSpec] {
  override def serialize(s: SnapshotSpec, g: JsonGenerator, p: SerializerProvider): Unit =
    SpecExpectationJson.writeEnvelope(g, s.`type`, s.query.version, s.query.timestamp, s.expectation) {}
}
private[workload] class SnapshotSpecDeserializer extends SpecDeserializer[SnapshotSpec] {
  override protected def fromNode(n: JsonNode): SnapshotSpec =
    SnapshotSpec(
      SnapshotQuery(SpecExpectationJson.optLong(n, "version"), SpecExpectationJson.optText(n, "timestamp")),
      SpecExpectationJson.read(n, classOf[SnapshotResult]))
}

// =============================================================================
// JSON and DataFrame utilities
// =============================================================================

/**
 * Serialize a Spark [[StructType]] as its Delta schema JSON (the `{type:struct,fields:[…]}` tree),
 * so write specs carry a typed schema the reader consumes directly.
 */
private class StructTypeSerializer extends JsonSerializer[StructType] {
  override def serialize(s: StructType, g: JsonGenerator, p: SerializerProvider): Unit =
    g.writeObject(JsonUtil.mapper.readTree(s.json))
}
private class StructTypeDeserializer extends JsonDeserializer[StructType] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): StructType =
    DataType.fromJson(jp.getCodec.readTree[JsonNode](jp).toString).asInstanceOf[StructType]
}

object JsonUtil {

  val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.registerModule(new SimpleModule()
      .addSerializer(classOf[StructType], new StructTypeSerializer)
      .addDeserializer(classOf[StructType], new StructTypeDeserializer))
    m.enable(DeserializationFeature.USE_LONG_FOR_INTS)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  private val prettyWriter = mapper.writerWithDefaultPrettyPrinter()

  def writeSpec(path: Path, spec: Any): Unit =
    Files.write(path, prettyWriter.writeValueAsBytes(spec))


  def readReadSpec(path: Path): ReadSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[ReadSpec])

  def readSnapshotSpec(path: Path): SnapshotSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[SnapshotSpec])

  /** Deserialize any spec by its `type` tag into the sealed [[Spec]]. */
  def readSpec(path: Path): Spec =
    mapper.readValue(Files.readAllBytes(path), classOf[Spec])

}
