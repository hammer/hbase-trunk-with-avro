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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.avro.generated.HBase;
import org.apache.hadoop.hbase.avro.generated.ADelete;
import org.apache.hadoop.hbase.avro.generated.AGet;
import org.apache.hadoop.hbase.avro.generated.APut;
import org.apache.hadoop.hbase.avro.generated.AScan;
import org.apache.hadoop.hbase.avro.generated.AResult;
import org.apache.hadoop.hbase.avro.generated.ACompressionAlgorithm;
import org.apache.hadoop.hbase.avro.generated.ATableDescriptor;
import org.apache.hadoop.hbase.avro.generated.AFamilyDescriptor;
import org.apache.hadoop.hbase.avro.generated.AColumnValue;
import org.apache.hadoop.hbase.avro.generated.AIOError;
import org.apache.hadoop.hbase.avro.generated.AIllegalArgument;
import org.apache.hadoop.hbase.avro.generated.ATableExists;
import org.apache.hadoop.hbase.avro.generated.AMasterNotRunning;

/**
 * Start an Avro server
 */
public class AvroServer {

  /**
   * The HBaseImpl is a glue object that connects Avro RPC calls to the
   * HBase client API primarily defined in the HBaseAdmin and HTable objects.
   */
  public static class HBaseImpl implements HBase {
    protected Configuration conf = null;
    protected HBaseAdmin admin = null;
    protected HTablePool htablePool = null;
    protected final Log LOG = LogFactory.getLog(this.getClass().getName());

    // nextScannerId and scannerMap are used to manage scanner state
    protected int nextScannerId = 0;
    protected HashMap<Integer, ResultScanner> scannerMap = null;

    //
    // UTILITY METHODS
    //

    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal
     * hash-map.
     *
     * @param scanner
     * @return integer scanner id
     */
    protected synchronized int addScanner(ResultScanner scanner) {
      int id = nextScannerId++;
      scannerMap.put(id, scanner);
      return id;
    }

    /**
     * Returns the scanner associated with the specified ID.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized ResultScanner getScanner(int id) {
      return scannerMap.get(id);
    }

    /**
     * Removes the scanner associated with the specified ID from the internal
     * id->scanner hash-map.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized ResultScanner removeScanner(int id) {
      return scannerMap.remove(id);
    }

    //
    // CTOR
    //

    // TODO(hammer): figure out how to set maxSize for HTablePool
    /**
     * Constructs an HBaseImpl object.
     * 
     * @throws MasterNotRunningException
     */
    HBaseImpl() throws MasterNotRunningException {
      conf = HBaseConfiguration.create();
      admin = new HBaseAdmin(conf);
      htablePool = new HTablePool(conf, 10);
      scannerMap = new HashMap<Integer, ResultScanner>();
    }

    //
    // SERVICE METHODS
    //

    public Void createTable(ATableDescriptor table) throws AIOError, 
                                                           AIllegalArgument,
                                                           ATableExists,
                                                           AMasterNotRunning {
      try {
	HTableDescriptor newTable = null;
	if (table.families != null) {
	  // TODO(hammer): use other AFD properties, not just name 
          newTable = new HTableDescriptor(Bytes.toBytes(table.name));
	  for (AFamilyDescriptor aNewFamily : table.families) {
	    newTable.addFamily(new HColumnDescriptor(Bytes.toBytes(aNewFamily.name)));
	  }
	} else {
	  newTable = new HTableDescriptor(Bytes.toBytes(table.name));
	}
        admin.createTable(newTable);
	return null;
      } catch (IllegalArgumentException e) {
	AIllegalArgument iae = new AIllegalArgument();
	iae.message = new Utf8(e.getMessage());
        throw iae;
      } catch (TableExistsException e) {
	ATableExists tee = new ATableExists();
	tee.message = new Utf8(e.getMessage());
        throw tee;
      } catch (MasterNotRunningException e) {
	AMasterNotRunning mnre = new AMasterNotRunning();
	mnre.message = new Utf8(e.getMessage());
        throw mnre;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // Note that disable, flush and major compaction of META needed in client
    // TODO(hammer): more selective cache dirtying than flush?
    public Void deleteTable(ByteBuffer table) throws AIOError {
      try {
	admin.deleteTable(Bytes.toBytes(table));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // NB: Asynchronous operation
    public Void modifyTable(ByteBuffer tableName, ATableDescriptor tableDescriptor) throws AIOError {
      try {
	admin.modifyTable(Bytes.toBytes(tableName),
                          AvroUtilities.atableDescToHTableDesc(tableDescriptor));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // TODO(hammer): handle regions too?
    // NB: Asynchronous operation
    public Void flush(ByteBuffer table) throws AIOError {
      try {
	admin.flush(Bytes.toBytes(table));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public Void addFamily(ByteBuffer table, AFamilyDescriptor family) throws AIOError {
      try {
	admin.addColumn(Bytes.toBytes(table), 
                        AvroUtilities.afamilyDescToHColumnDesc(family));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // NB: Asynchronous operation
    public Void deleteFamily(ByteBuffer table, ByteBuffer family) throws AIOError {
      try {
	admin.deleteColumn(Bytes.toBytes(table), Bytes.toBytes(family));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // NB: Asynchronous operation
    public Void modifyFamily(ByteBuffer table, ByteBuffer familyName, AFamilyDescriptor familyDescriptor) throws AIOError {
      try {
	admin.modifyColumn(Bytes.toBytes(table), Bytes.toBytes(familyName),
                           AvroUtilities.afamilyDescToHColumnDesc(familyDescriptor));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public Void enableTable(ByteBuffer table) throws AIOError {
      try {
	admin.enableTable(Bytes.toBytes(table));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }
    
    public Void disableTable(ByteBuffer table) throws AIOError {
      try {
	admin.disableTable(Bytes.toBytes(table));
	return null;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }
    
    public boolean tableExists(ByteBuffer table) throws AIOError {
      try {
	return admin.tableExists(Bytes.toBytes(table));
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public boolean isTableEnabled(ByteBuffer table) throws AIOError {
      try {
	return HTable.isTableEnabled(Bytes.toBytes(table));
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public Utf8 getHBaseVersion() throws AIOError {
      try {
	return new Utf8(admin.getClusterStatus().getHBaseVersion());
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // TODO(hammer): Handle the case where the family does not exist better?
    public AFamilyDescriptor describeFamily(ByteBuffer table, ByteBuffer family) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      AFamilyDescriptor afamily = new AFamilyDescriptor();
      try {
	HTableDescriptor htd = htable.getTableDescriptor();
        HColumnDescriptor hcd = htd.getFamily(Bytes.toBytes(family));
	afamily = AvroUtilities.hcolumnDescToAFamilyDesc(hcd);
      } catch (IOException e) {
        AIOError ioe = new AIOError();
        ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        htablePool.putTable(htable);
      }
      return afamily;
    }

    // TODO(hammer): Better to use HBaseAdmin or HTableInterface?
    // TODO(hammer): Handle the case where the table does not exist better?
    public ATableDescriptor describeTable(ByteBuffer table) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      ATableDescriptor atd = new ATableDescriptor();
      try {
	HTableDescriptor htd = htable.getTableDescriptor();
        atd = AvroUtilities.htableDescToATableDesc(htd);
      } catch (IOException e) {
        AIOError ioe = new AIOError();
        ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        htablePool.putTable(htable);
      }
      return atd;
    }

    public GenericArray<ATableDescriptor> listTables() throws AIOError {
      try {
        HTableDescriptor[] tables = this.admin.listTables();
	Schema atdSchema = Schema.createArray(ATableDescriptor.SCHEMA$);
        GenericData.Array<ATableDescriptor> result = null;
	result = new GenericData.Array<ATableDescriptor>(tables.length, atdSchema);
        for (HTableDescriptor table : tables) {
	  ATableDescriptor atd = AvroUtilities.htableDescToATableDesc(table);
	  result.add(atd);
	}
        return result;
      } catch (IOException e) {
	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    // TODO(hammer): Can Get have timestamp and timerange?
    // TODO(hammer): Consider handling ByteBuffers as well as byte[]s
    // TODO(hammer): Do I need to catch the RuntimeException of getTable?
    // TODO(hammer): Handle gets with no results
    public AResult get(ByteBuffer table, AGet aget) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      AResult aresult = new AResult();
      aresult.row = aget.row;
      aresult.entries = null;
      try {
	Get get = AvroUtilities.agetToGet(aget);
        Result result = htable.get(get);
	aresult.entries = AvroUtilities.resultToEntries(result);
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        htablePool.putTable(htable);
      }
      return aresult;
    }

    // TODO(hammer): Java with statement for htablepool concision?
    public Void put(ByteBuffer table, APut aput) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      try {
        Put put = new Put(Bytes.toBytes(aput.row));
        for (AColumnValue acv : aput.columnValues) {
  	  if (acv.timestamp != null) {
	    put.add(Bytes.toBytes(acv.family),
                    Bytes.toBytes(acv.qualifier),
                    acv.timestamp,
		    Bytes.toBytes(acv.value));
	  } else {
	    put.add(Bytes.toBytes(acv.family),
                    Bytes.toBytes(acv.qualifier),
		    Bytes.toBytes(acv.value));
	  }
        }
        htable.put(put);
      } catch (IOException e) {
        AIOError ioe = new AIOError();
        ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        htablePool.putTable(htable);
      }
      return null;
    }

    public int scannerOpen(ByteBuffer table, AScan ascan) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      try {
        Scan scan = AvroUtilities.scanFromAScan(ascan);
        return addScanner(htable.getScanner(scan));
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        htablePool.putTable(htable);
      }
    }

    public Void scannerClose(int scannerId) throws AIOError, AIllegalArgument {
      try {
        ResultScanner scanner = getScanner(scannerId);
        if (scanner == null) {
      	  AIllegalArgument aie = new AIllegalArgument();
	  aie.message = new Utf8("scanner ID is invalid: " + scannerId);
          throw aie;
        }
        scanner.close();
        removeScanner(scannerId);
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
      return null;
    }

    public GenericArray<AResult> scannerGetRows(int scannerId, int numberOfRows) throws AIOError, AIllegalArgument {
      try {
        ResultScanner scanner = getScanner(scannerId);
        if (scanner == null) {
      	  AIllegalArgument aie = new AIllegalArgument();
	  aie.message = new Utf8("scanner ID is invalid: " + scannerId);
          throw aie;
        }
        Result[] results = null;
        return AvroUtilities.aresultsFromResults(scanner.next(numberOfRows));
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      }
    }

    public Void delete(ByteBuffer table, ADelete adelete) throws AIOError {
      HTableInterface htable = htablePool.getTable(Bytes.toBytes(table));
      try {
	Delete delete = AvroUtilities.adeleteToDelete(adelete);
        htable.delete(delete);
        return null;
      } catch (IOException e) {
    	AIOError ioe = new AIOError();
	ioe.message = new Utf8(e.getMessage());
        throw ioe;
      } finally {
        htablePool.putTable(htable);
      }
    }
  }

  //
  // MAIN PROGRAM
  //

  private static void printUsageAndExit() {
    printUsageAndExit(null);
  }
  
  private static void printUsageAndExit(final String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.out.println("Usage: java org.apache.hadoop.hbase.avro.AvroServer " +
      "--help | [--port=PORT] start");
    System.out.println("Arguments:");
    System.out.println(" start Start Avro server");
    System.out.println(" stop  Stop Avro server");
    System.out.println("Options:");
    System.out.println(" port  Port to listen on. Default: 9090");
    System.out.println(" help  Print this message and exit");
    System.exit(0);
  }

  // TODO(hammer): Figure out a better way to keep the server alive!
  protected static void doMain(final String[] args) throws Exception {
    if (args.length < 1) {
      printUsageAndExit();
    }
    int port = 9090;
    final String portArgKey = "--port=";
    for (String cmd: args) {
      if (cmd.startsWith(portArgKey)) {
        port = Integer.parseInt(cmd.substring(portArgKey.length()));
        continue;
      } else if (cmd.equals("--help") || cmd.equals("-h")) {
        printUsageAndExit();
      } else if (cmd.equals("start")) {
        continue;
      } else if (cmd.equals("stop")) {
        printUsageAndExit("To shutdown the Avro server run " +
          "bin/hbase-daemon.sh stop avro or send a kill signal to " +
          "the Avro server pid");
      }
      
      // Print out usage if we get to here.
      printUsageAndExit();
    }
    Log LOG = LogFactory.getLog("AvroServer");
    LOG.info("starting HBase Avro server on port " + Integer.toString(port));
    SpecificResponder r = new SpecificResponder(HBase.class, new HBaseImpl());
    HttpServer server = new HttpServer(r, 9090);
    Thread.sleep(1000000);
  }

  // TODO(hammer): Don't eat it after a single exception
  // TODO(hammer): Figure out why we do doMain()
  // TODO(hammer): Figure out if we want String[] or String []
  public static void main(String[] args) throws Exception {
    doMain(args);
  }
}
