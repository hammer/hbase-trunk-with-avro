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
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.avro.generated.AColumn;
import org.apache.hadoop.hbase.avro.generated.ADelete;
import org.apache.hadoop.hbase.avro.generated.AGet;
import org.apache.hadoop.hbase.avro.generated.AScan;
import org.apache.hadoop.hbase.avro.generated.AResult;
import org.apache.hadoop.hbase.avro.generated.AResultEntry;
import org.apache.hadoop.hbase.avro.generated.AFamilyDescriptor;
import org.apache.hadoop.hbase.avro.generated.ATableDescriptor;
import org.apache.hadoop.hbase.avro.generated.ACompressionAlgorithm;

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
  static public Get agetToGet(AGet aget) throws IOException {
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

  static public AFamilyDescriptor hcolumnDescToAFamilyDesc(HColumnDescriptor hcd) throws IOException {
    AFamilyDescriptor afamily = new AFamilyDescriptor();
    afamily.name = ByteBuffer.wrap(hcd.getName());
    String compressionAlgorithm = hcd.getCompressionType().getName();
    if (compressionAlgorithm == "LZO") {
      afamily.compression = ACompressionAlgorithm.LZO;
    } else if (compressionAlgorithm == "GZ") {
      afamily.compression = ACompressionAlgorithm.GZ;
    } else {
      afamily.compression = ACompressionAlgorithm.NONE;
    }
    afamily.maxVersions = hcd.getMaxVersions();
    afamily.blocksize = hcd.getBlocksize();
    afamily.inMemory = hcd.isInMemory();
    afamily.timeToLive = hcd.getTimeToLive();
    afamily.blockCacheEnabled = hcd.isBlockCacheEnabled();
    return afamily;
  }

  static public ATableDescriptor htableDescToATableDesc(HTableDescriptor table) throws IOException {
    ATableDescriptor atd = new ATableDescriptor();
    atd.name = ByteBuffer.wrap(table.getName());
    Collection<HColumnDescriptor> families = table.getFamilies();
    if (families.size() > 0) {
      Schema afdSchema = Schema.createArray(AFamilyDescriptor.SCHEMA$);
      GenericData.Array<AFamilyDescriptor> afamilies = null;
      afamilies = new GenericData.Array<AFamilyDescriptor>(families.size(), afdSchema);
      for (HColumnDescriptor hcd : families) {
	AFamilyDescriptor afamily = hcolumnDescToAFamilyDesc(hcd);
        afamilies.add(afamily);
      }
      atd.families = afamilies;
    }
    atd.maxFileSize = table.getMaxFileSize();
    atd.memStoreFlushSize = table.getMemStoreFlushSize();
    atd.rootRegion = table.isRootRegion();
    atd.metaRegion = table.isMetaRegion();
    atd.metaTable = table.isMetaTable();
    atd.readOnly = table.isReadOnly();
    atd.deferredLogFlush = table.isDeferredLogFlush();
    return atd;
  }

  static public Scan scanFromAScan(AScan ascan) throws IOException {
    Scan scan = new Scan();
    if (ascan.startRow != null) {
      scan.setStartRow(Bytes.toBytes(ascan.startRow));
    }
    if (ascan.stopRow != null) {
      scan.setStopRow(Bytes.toBytes(ascan.stopRow));
    }
    if (ascan.columns != null) {
      for (AColumn acolumn : ascan.columns) {
	if (acolumn.qualifier != null) {
	  scan.addColumn(Bytes.toBytes(acolumn.family), Bytes.toBytes(acolumn.qualifier));
	} else {
	  scan.addFamily(Bytes.toBytes(acolumn.family));
	}
      }
    }
    if (ascan.timestamp != null) {
      scan.setTimeStamp(ascan.timestamp);
    }
    if (ascan.timerange != null) {
      scan.setTimeRange(ascan.timerange.minStamp, ascan.timerange.maxStamp);
    }
    if (ascan.maxVersions != null) {
      scan.setMaxVersions(ascan.maxVersions);
    }
    return scan;
  }

  static public GenericArray<AResult> aresultsFromResults(Result[] results) {
    Schema s = Schema.createArray(AResult.SCHEMA$);
    GenericData.Array<AResult> aresults = null;
    if (results != null && results.length > 0) {
      aresults = new GenericData.Array<AResult>(results.length, s);
      for (Result result : results) {
	AResult aresult = new AResult();
        aresult.row = ByteBuffer.wrap(result.getRow());
        aresult.entries = resultToEntries(result);
	aresults.add(aresult);
      }
    } else {
      aresults = new GenericData.Array<AResult>(0, s);
    }
    return aresults;
  }

  static public Delete adeleteToDelete(ADelete adelete) throws IOException {
    Delete delete = new Delete(Bytes.toBytes(adelete.row));
    if (adelete.columns != null) {
      for (AColumn acolumn : adelete.columns) {
	if (acolumn.qualifier != null) {
	  delete.deleteColumns(Bytes.toBytes(acolumn.family), Bytes.toBytes(acolumn.qualifier));
	} else {
	  delete.deleteFamily(Bytes.toBytes(acolumn.family));
	}
      }
    }
    return delete;
  }
}