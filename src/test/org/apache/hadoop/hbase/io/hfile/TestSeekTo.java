/**
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test {@link HFileScanner#seekTo(byte[])} and its variants.
 */
public class TestSeekTo extends TestCase {
  private static String ROOT_DIR =
    System.getProperty("test.build.data", "/tmp/TestHFile");

  private HBaseConfiguration conf;
  private LocalFileSystem fs;

  @Override
  public void setUp() {
    conf = new HBaseConfiguration();
    RawLocalFileSystem rawLFS = new RawLocalFileSystem();
    rawLFS.setConf(conf);
    fs = new LocalFileSystem(rawLFS);
  }
  private FSDataOutputStream createFSOutput(Path name) throws IOException {
    if (fs.exists(name)) fs.delete(name, true);
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

  Path makeNewFile() throws IOException {
    Path ncTFile = new Path(ROOT_DIR, "basic.hfile");
    FSDataOutputStream fout = createFSOutput(ncTFile);
    HFile.Writer writer = new HFile.Writer(fout, 40, "none", null);
    // 4 bytes * 3 * 2 for each key/value +
    // 3 for keys, 15 for values = 42 (woot)
    writer.append(Bytes.toBytes("c"), Bytes.toBytes("value"));
    writer.append(Bytes.toBytes("e"), Bytes.toBytes("value"));
    writer.append(Bytes.toBytes("g"), Bytes.toBytes("value"));
    // block transition
    writer.append(Bytes.toBytes("i"), Bytes.toBytes("value"));
    writer.append(Bytes.toBytes("k"), Bytes.toBytes("value"));
    writer.close();
    fout.close();
    return ncTFile;
  }
  public void testSeekBefore() throws Exception {
    Path p = makeNewFile();
    HFile.Reader reader = new HFile.Reader(fs, p, null);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner();
    assertEquals(false, scanner.seekBefore(Bytes.toBytes("a")));
    
    assertEquals(false, scanner.seekBefore(Bytes.toBytes("c")));
    
    assertEquals(true, scanner.seekBefore(Bytes.toBytes("d")));
    assertEquals("c", scanner.getKeyString());
    
    assertEquals(true, scanner.seekBefore(Bytes.toBytes("e")));
    assertEquals("c", scanner.getKeyString());
    
    assertEquals(true, scanner.seekBefore(Bytes.toBytes("f")));
    assertEquals("e", scanner.getKeyString());
    
    assertEquals(true, scanner.seekBefore(Bytes.toBytes("g")));
    assertEquals("e", scanner.getKeyString());
    
    assertEquals(true, scanner.seekBefore(Bytes.toBytes("h")));
    assertEquals("g", scanner.getKeyString());
    assertEquals(true, scanner.seekBefore(Bytes.toBytes("i")));
    assertEquals("g", scanner.getKeyString());
    assertEquals(true, scanner.seekBefore(Bytes.toBytes("j")));
    assertEquals("i", scanner.getKeyString());
    assertEquals(true, scanner.seekBefore(Bytes.toBytes("k")));
    assertEquals("i", scanner.getKeyString());
    assertEquals(true, scanner.seekBefore(Bytes.toBytes("l")));
    assertEquals("k", scanner.getKeyString());
  }
  
  public void testSeekTo() throws Exception {
    Path p = makeNewFile();
    HFile.Reader reader = new HFile.Reader(fs, p, null);
    reader.loadFileInfo();
    assertEquals(2, reader.blockIndex.count);
    HFileScanner scanner = reader.getScanner();
    // lies before the start of the file.
    assertEquals(-1, scanner.seekTo(Bytes.toBytes("a")));
  
    assertEquals(1, scanner.seekTo(Bytes.toBytes("d")));
    assertEquals("c", scanner.getKeyString());
    
    // Across a block boundary now.
    assertEquals(1, scanner.seekTo(Bytes.toBytes("h")));
    assertEquals("g", scanner.getKeyString());
    
    assertEquals(1, scanner.seekTo(Bytes.toBytes("l")));
    assertEquals("k", scanner.getKeyString());
  }
  
  public void testBlockContainingKey() throws Exception {
    Path p = makeNewFile();
    HFile.Reader reader = new HFile.Reader(fs, p, null);
    reader.loadFileInfo();
    System.out.println(reader.blockIndex.toString());
    // falls before the start of the file.
    assertEquals(-1, reader.blockIndex.blockContainingKey(Bytes.toBytes("a")));
    assertEquals(0, reader.blockIndex.blockContainingKey(Bytes.toBytes("c")));
    assertEquals(0, reader.blockIndex.blockContainingKey(Bytes.toBytes("d")));
    assertEquals(0, reader.blockIndex.blockContainingKey(Bytes.toBytes("e")));
    assertEquals(0, reader.blockIndex.blockContainingKey(Bytes.toBytes("g")));
    assertEquals(0, reader.blockIndex.blockContainingKey(Bytes.toBytes("h")));
    assertEquals(1, reader.blockIndex.blockContainingKey(Bytes.toBytes("i")));
    assertEquals(1, reader.blockIndex.blockContainingKey(Bytes.toBytes("j")));
    assertEquals(1, reader.blockIndex.blockContainingKey(Bytes.toBytes("k")));
    assertEquals(1, reader.blockIndex.blockContainingKey(Bytes.toBytes("l")));


    
  }
}