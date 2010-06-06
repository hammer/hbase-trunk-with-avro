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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.hbase.avro.generated.ATableDescriptor;

/**
 * Unit testing for AvroServer.HBaseImpl, a part of the
 * org.apache.hadoop.hbase.avro package.
 */
public class TestAvroServer {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // Static names for tables, columns, rows, and values
  // TODO(hammer): Better style to define these in test method?
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
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  /**
   * Tests for creating, enabling, disabling, and deleting tables.
   *
   * @throws Exception
   */
  @Test
  public void testTableAndAdmin() throws Exception {
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
