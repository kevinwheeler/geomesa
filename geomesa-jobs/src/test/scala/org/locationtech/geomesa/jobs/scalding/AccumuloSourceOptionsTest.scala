/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.jobs.scalding

import com.twitter.scalding.{Hdfs, Read, Write}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloSourceOptionsTest extends Specification {

  "AccumuloSourceOptions" should {
    "serialize strings/ranges" >> {
      "with inclusive bounds" >> {
        val ranges = SerializedRange.parse("[a b c,d e f]").head
        ranges.start mustEqual Endpoint(Some("a"), Some("b"), Some("c"), true)
        ranges.end mustEqual Endpoint(Some("d"), Some("e"), Some("f"), true)
      }
      "with exclusive bounds" >> {
        val ranges = SerializedRange.parse("(a b c,d e f)").head
        ranges.start mustEqual Endpoint(Some("a"), Some("b"), Some("c"), false)
        ranges.end mustEqual Endpoint(Some("d"), Some("e"), Some("f"), false)
      }
      "with optional column qualifiers" >> {
        val ranges = SerializedRange.parse("(a b,d e)").head
        ranges.start mustEqual Endpoint(Some("a"), Some("b"), None, false)
        ranges.end mustEqual Endpoint(Some("d"), Some("e"), None, false)
      }
      "with optional column families" >> {
        val ranges = SerializedRange.parse("(a,d)").head
        ranges.start mustEqual Endpoint(Some("a"), None, None, false)
        ranges.end mustEqual Endpoint(Some("d"), None, None, false)
      }
      "with extra whitespace" >> {
        val ranges = SerializedRange.parse(" (a b c , d e f ) ").head
        ranges.start mustEqual Endpoint(Some("a"), Some("b"), Some("c"), false)
        ranges.end mustEqual Endpoint(Some("d"), Some("e"), Some("f"), false)
      }
      "with no start" >> {
        val ranges = SerializedRange.parse("(,d e f)").head
        ranges.start mustEqual Endpoint(None, None, None, false)
        ranges.end mustEqual Endpoint(Some("d"), Some("e"), Some("f"), false)
      }
      "with no end" >> {
        val ranges = SerializedRange.parse("(a b c,)").head
        ranges.start mustEqual Endpoint(Some("a"), Some("b"), Some("c"), false)
        ranges.end mustEqual Endpoint(None, None, None, false)
      }
      "with multiple ranges" >> {
        val ranges = SerializedRange.parse("(a b c,d e f),[h i j, k l m]")
        ranges must haveSize(2)
        ranges(0).start mustEqual Endpoint(Some("a"), Some("b"), Some("c"), false)
        ranges(0).end mustEqual Endpoint(Some("d"), Some("e"), Some("f"), false)
        ranges(1).start mustEqual Endpoint(Some("h"), Some("i"), Some("j"), true)
        ranges(1).end mustEqual Endpoint(Some("k"), Some("l"), Some("m"), true)
      }
    }
    "unserialize strings/ranges" >> {
      val ranges = SerializedRange.parse("(a b c,d e f),[h i j, k l m]")
      val collect = ranges.collect { case SerializedRangeSeq(r) => r }
      collect must haveSize(2)
      collect(0).getStartKey.getRow.toString mustEqual "a"
      collect(0).getStartKey.getColumnFamily.toString mustEqual "b"
      collect(0).getStartKey.getColumnQualifier.toString mustEqual "c"
      collect(0).getEndKey.getRow.toString mustEqual "d"
      collect(0).getEndKey.getColumnFamily.toString mustEqual "e"
      collect(0).getEndKey.getColumnQualifier.toString mustEqual "f"
      collect(0).isStartKeyInclusive must beFalse
      collect(0).isEndKeyInclusive must beFalse
      collect(1).getStartKey.getRow.toString mustEqual "h"
      collect(1).getStartKey.getColumnFamily.toString mustEqual "i"
      collect(1).getStartKey.getColumnQualifier.toString mustEqual "j"
      collect(1).getEndKey.getRow.toString mustEqual "k"
      collect(1).getEndKey.getColumnFamily.toString mustEqual "l"
      collect(1).getEndKey.getColumnQualifier.toString mustEqual "m"
      collect(1).isStartKeyInclusive must beTrue
      collect(1).isEndKeyInclusive must beTrue
    }
    "serialize strings/columns" >> {
      "with a single pair" >> {
        val cols = SerializedColumn.parse("[a b]").head
        cols.cf mustEqual "a"
        cols.cq mustEqual "b"
      }
      "with multiple pairs" >> {
        val cols = SerializedColumn.parse("[a b],[c d]")
        cols must haveSize(2)
        cols(0).cf mustEqual "a"
        cols(0).cq mustEqual "b"
        cols(1).cf mustEqual "c"
        cols(1).cq mustEqual "d"
      }
      "with whitespace" >> {
        val cols = SerializedColumn.parse(" [a b] , [ c d ] ")
        cols must haveSize(2)
        cols(0).cf mustEqual "a"
        cols(0).cq mustEqual "b"
        cols(1).cf mustEqual "c"
        cols(1).cq mustEqual "d"
      }
    }
  }
}
