/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.avro.generated.AColumn;
import org.apache.hadoop.hbase.avro.generated.AGet;
import org.apache.hadoop.hbase.avro.generated.AResultEntry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;

public class AvroUtilities {

  // TODO(hammer): More concise idiom than if not null assign?
  /**
   * Convert an AGet to a Get.
   * 
   * @param aget AGet
   * @return HBase client Get object
   */
  static public Get agetToGet(AGet aget) throws IOException{
    Get get = new Get(Bytes.toBytes(aget.row));
    if (aget.columns != null) {
      for (AColumn acolumn : aget.columns) {
	if (acolumn.qualifier != null) {
	  get.addColumn(Bytes.toBytes(acolumn.family), Bytes.toBytes(acolumn.qualifier));
	} else {
	  get.addFamily(Bytes.toBytes(acolumn.family));
	}
      }
    }
    if (aget.timestamp != null) {
      get.setTimeStamp(aget.timestamp);
    }
    if (aget.timerange != null) {
      get.setTimeRange(aget.timerange.minStamp, aget.timerange.maxStamp);
    }
    if (aget.maxVersions != null) {
      get.setMaxVersions(aget.maxVersions);
    }
    return get;
  }

  // TODO(hammer): Pick one: Timestamp or TimeStamp
  /**
   * Grab the entries from a Result object
   * 
   * @param result Result
   * @return entries array
   */
    static public GenericArray<AResultEntry> resultToEntries(Result result) {
      Schema s = Schema.createArray(AResultEntry.SCHEMA$);
      GenericData.Array<AResultEntry> entries = null;
      List<KeyValue> resultKeyValues = result.list();
      if (resultKeyValues.size() > 0) {
	entries = new GenericData.Array<AResultEntry>(resultKeyValues.size(), s);
        for (KeyValue resultKeyValue : resultKeyValues) {
	  AResultEntry entry = new AResultEntry();
	  entry.family = ByteBuffer.wrap(resultKeyValue.getFamily());
	  entry.qualifier = ByteBuffer.wrap(resultKeyValue.getQualifier());
	  entry.value = ByteBuffer.wrap(resultKeyValue.getValue());
	  entry.timestamp = resultKeyValue.getTimestamp();
	  entries.add(entry);
	}
      }
      return entries;
    }
}