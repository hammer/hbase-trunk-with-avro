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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;

import org.apache.hadoop.hbase.avro.generated.AFamilyDescriptor;
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
  private static ByteBuffer familyAname = ByteBuffer.wrap(Bytes.toBytes("FamilyA"));

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
   * Tests for creating, enabling, disabling, modifying, and deleting tables.
   *
   * @throws Exception
   */
  @Test
  public void testTableAdmin() throws Exception {
    AvroServer.HBaseImpl impl = new AvroServer.HBaseImpl();

    assertEquals(impl.listTables().size(), 0);

    ATableDescriptor tableA = new ATableDescriptor();
    tableA.name = tableAname;
    impl.createTable(tableA);
    assertEquals(impl.listTables().size(), 1);
    assertTrue(impl.isTableEnabled(tableAname));
    assertTrue(impl.tableExists(tableAname));

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

    tableA.maxFileSize = 123456L;
    impl.modifyTable(tableAname, tableA);
    assertEquals((long) impl.describeTable(tableAname).maxFileSize, 123456L);

    impl.enableTable(tableAname);
    assertTrue(impl.isTableEnabled(tableAname));
    impl.disableTable(tableAname);
    impl.deleteTable(tableAname);
  }
  /**
   * Tests for creating, modifying, and deleting column families.
   *
   * @throws Exception
   */
  @Test
  public void testFamilyAdmin() throws Exception {
    AvroServer.HBaseImpl impl = new AvroServer.HBaseImpl();

    ATableDescriptor tableA = new ATableDescriptor();
    tableA.name = tableAname;
    AFamilyDescriptor familyA = new AFamilyDescriptor();
    familyA.name = familyAname;
    Schema familyArraySchema = Schema.createArray(AFamilyDescriptor.SCHEMA$);
    GenericArray<AFamilyDescriptor> families = new GenericData.Array<AFamilyDescriptor>(1, familyArraySchema);
    families.add(familyA);
    tableA.families = families;
    impl.createTable(tableA);    
    assertEquals(impl.describeTable(tableAname).families.size(), 1);
  }
}
