/*
 * Copyright 2009 The Apache Software Foundation
 *
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.avro.generated.ATableDescriptor;

/**
 * Unit testing for AvroServer.HBaseImpl, a part of the
 * org.apache.hadoop.hbase.avro package.
 */
public class TestAvroServer extends HBaseClusterTestCase {

  // Static names for tables, columns, rows, and values
  private static ByteBuffer tableAname = ByteBuffer.wrap(Bytes.toBytes("tableA"));
  private static ByteBuffer tableBname = ByteBuffer.wrap(Bytes.toBytes("tableB"));
  private static ByteBuffer columnAname = ByteBuffer.wrap(Bytes.toBytes("ColumnA"));
  private static ByteBuffer columnBname = ByteBuffer.wrap(Bytes.toBytes("ColumnB"));
  private static ByteBuffer rowAname = ByteBuffer.wrap(Bytes.toBytes("rowA"));
  private static ByteBuffer rowBname = ByteBuffer.wrap(Bytes.toBytes("rowB"));
  private static ByteBuffer valueAname = ByteBuffer.wrap(Bytes.toBytes("valueA"));
  private static ByteBuffer valueBname = ByteBuffer.wrap(Bytes.toBytes("valueB"));
  private static ByteBuffer valueCname = ByteBuffer.wrap(Bytes.toBytes("valueC"));
  private static ByteBuffer valueDname = ByteBuffer.wrap(Bytes.toBytes("valueD"));

  /**
   * Runs all of the tests under a single JUnit test method.  We
   * consolidate all testing to one method because HBaseClusterTestCase
   * is prone to OutOfMemoryExceptions when there are three or more
   * JUnit test methods.
   *
   * @throws Exception
   */
  public void testAll() throws Exception {
    //doTestClusterMetadata();
    //doTestTableMetadata();
    //doTestFamilyMetadata();
    doTestTableAdmin();
    //doTestFamilyAdmin();
    //doTestSingleRowDML();
    //doTestMultiRowDML();
  }

  /**
   * Tests for creating, enabling, disabling, and deleting tables.
   *
   * @throws Exception
   */
  public void doTestTableAdmin() throws Exception {
    AvroServer.HBaseImpl impl = new AvroServer.HBaseImpl();

    assertEquals(impl.listTables().size(), 0);

    ATableDescriptor tableA = new ATableDescriptor();
    tableA.name = tableAname;
    impl.createTable(tableA);
    assertEquals(impl.listTables().size(), 1);
    assertTrue(impl.isTableEnabled(tableAname));

    ATableDescriptor tableB = new ATableDescriptor();
    tableB.name = tableBname;
    impl.createTable(tableB);
    assertEquals(impl.listTables().size(), 2);

    impl.disableTable(tableBname);
    assertFalse(impl.isTableEnabled(tableBname));

    impl.deleteTable(tableBname);
    assertEquals(impl.listTables().size(), 1);

    impl.disableTable(tableAname);
    assertFalse(impl.isTableEnabled(tableAname));
    impl.enableTable(tableAname);
    assertTrue(impl.isTableEnabled(tableAname));
    impl.disableTable(tableAname);
    impl.deleteTable(tableAname);
  }
}
