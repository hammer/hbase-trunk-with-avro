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

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.avro.generated.AClusterStatus;
import org.apache.hadoop.hbase.avro.generated.AServerInfo;
import org.apache.hadoop.hbase.avro.generated.AServerLoad;
import org.apache.hadoop.hbase.avro.generated.ARegionLoad;
import org.apache.hadoop.hbase.avro.generated.AServerAddress;
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
import org.apache.avro.util.Utf8;
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

  static public HColumnDescriptor afamilyDescToHColumnDesc(AFamilyDescriptor afd) throws IOException {
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(afd.name));

    ACompressionAlgorithm compressionAlgorithm = afd.compression;
    if (compressionAlgorithm == ACompressionAlgorithm.LZO) {
      hcd.setCompressionType(Compression.Algorithm.LZO);
    } else if (compressionAlgorithm == ACompressionAlgorithm.GZ) {
      hcd.setCompressionType(Compression.Algorithm.GZ);
    } else {
      hcd.setCompressionType(Compression.Algorithm.NONE);
    }

    if (afd.maxVersions != null) {
      hcd.setMaxVersions(afd.maxVersions);
    }

    if (afd.blocksize != null) {
      hcd.setBlocksize(afd.blocksize);
    }

    if (afd.inMemory != null) {
      hcd.setInMemory(afd.inMemory);
    }

    if (afd.timeToLive != null) {
      hcd.setTimeToLive(afd.timeToLive);
    }

    if (afd.blockCacheEnabled != null) {
      hcd.setBlockCacheEnabled(afd.blockCacheEnabled);
    }
    return hcd;
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

  // TODO(hammer): warn if user tries to manipulate meta flags?
  static public HTableDescriptor atableDescToHTableDesc(ATableDescriptor atd) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(Bytes.toBytes(atd.name));

    if (atd.families != null && atd.families.size() > 0) {
      for (AFamilyDescriptor afd : atd.families) {
	htd.addFamily(afamilyDescToHColumnDesc(afd));
      }
    }

    if (atd.maxFileSize != null) {
      htd.setMaxFileSize(atd.maxFileSize);
    }

    if (atd.memStoreFlushSize != null) {
      htd.setMemStoreFlushSize(atd.memStoreFlushSize);
    }

    if (atd.readOnly != null) {
      htd.setReadOnly(atd.readOnly);
    }

    if (atd.deferredLogFlush != null) {
      htd.setDeferredLogFlush(atd.deferredLogFlush);
    }

    return htd;
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

  static public AServerAddress hserverAddressToAServerAddress(HServerAddress hsa) throws IOException {
    AServerAddress asa = new AServerAddress();
    asa.bindAddress = new Utf8(hsa.getBindAddress());
    asa.hostname = new Utf8(hsa.getHostname());
    asa.inetSocketAddress = new Utf8(hsa.getInetSocketAddress().toString());
    asa.port = hsa.getPort();
    return asa;
  }

  static public ARegionLoad hregionLoadToARegionLoad(HServerLoad.RegionLoad rl) throws IOException {
    ARegionLoad arl = new ARegionLoad();
    arl.memStoreSizeMB = rl.getMemStoreSizeMB();
    arl.name = ByteBuffer.wrap(rl.getName());
    arl.storefileIndexSizeMB = rl.getStorefileIndexSizeMB();
    arl.storefiles = rl.getStorefiles();
    arl.storefileSizeMB = rl.getStorefileSizeMB();
    arl.stores = rl.getStores();
    return arl;
  }

  static public AServerLoad hserverLoadToAServerLoad(HServerLoad hsl) throws IOException {
    AServerLoad asl = new AServerLoad();
    asl.load = hsl.getLoad();
    asl.maxHeapMB = hsl.getMaxHeapMB();
    asl.memStoreSizeInMB = hsl.getMemStoreSizeInMB();
    asl.numberOfRegions = hsl.getNumberOfRegions();
    asl.numberOfRequests = hsl.getNumberOfRequests();

    Collection<HServerLoad.RegionLoad> regionLoads = hsl.getRegionsLoad();
    Schema s = Schema.createArray(ARegionLoad.SCHEMA$);
    GenericData.Array<ARegionLoad> aregionLoads = null;
    if (regionLoads != null) {
      aregionLoads = new GenericData.Array<ARegionLoad>(regionLoads.size(), s);
      for (HServerLoad.RegionLoad rl : regionLoads) {
	aregionLoads.add(hregionLoadToARegionLoad(rl));
      }
    }
    asl.regionsLoad = aregionLoads;

    asl.storefileIndexSizeInMB = hsl.getStorefileIndexSizeInMB();
    asl.storefiles = hsl.getStorefiles();
    asl.storefileSizeInMB = hsl.getStorefileSizeInMB();
    asl.usedHeapMB = hsl.getUsedHeapMB();
    return asl;
  }

  static public AServerInfo hserverInfoToAServerInfo(HServerInfo hsi) throws IOException {
    AServerInfo asi = new AServerInfo();
    asi.infoPort = hsi.getInfoPort();
    asi.load = hserverLoadToAServerLoad(hsi.getLoad());
    asi.serverAddress = hserverAddressToAServerAddress(hsi.getServerAddress());
    asi.serverName = new Utf8(hsi.getServerName());
    asi.startCode = hsi.getStartCode();
    return asi;
  }

  static public AClusterStatus clusterStatusToAClusterStatus(ClusterStatus cs) throws IOException {
    AClusterStatus acs = new AClusterStatus();
    acs.averageLoad = cs.getAverageLoad();
    Collection<String> deadServerNames = cs.getDeadServerNames();
    if (deadServerNames != null) {
      Schema stringArraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
      acs.deadServerNames = new GenericData.Array<Utf8>(deadServerNames.size(), stringArraySchema);
      for (String deadServerName : deadServerNames) {
	acs.deadServerNames.add(new Utf8(deadServerName));
      }
    }
    acs.deadServers = cs.getDeadServers();
    acs.hbaseVersion = new Utf8(cs.getHBaseVersion());
    acs.regionsCount = cs.getRegionsCount();
    acs.requestsCount = cs.getRequestsCount();
    Collection<HServerInfo> hserverInfos = cs.getServerInfo();
    Schema s = Schema.createArray(AServerInfo.SCHEMA$);
    GenericData.Array<AServerInfo> aserverInfos = null;
    if (hserverInfos != null) {
      aserverInfos = new GenericData.Array<AServerInfo>(hserverInfos.size(), s);
      for (HServerInfo hsi : hserverInfos) {
	aserverInfos.add(hserverInfoToAServerInfo(hsi));
      }
    }
    acs.serverInfos = aserverInfos;
    acs.servers = cs.getServers();
    return acs;
  }
}