package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public interface HBase {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"HBase\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"types\":[{\"type\":\"enum\",\"name\":\"ACompressionAlgorithm\",\"symbols\":[\"LZO\",\"GZ\",\"NONE\"]},{\"type\":\"record\",\"name\":\"AFamilyDescriptor\",\"fields\":[{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"compression\",\"type\":[\"ACompressionAlgorithm\",\"null\"]},{\"name\":\"maxVersions\",\"type\":[\"int\",\"null\"]},{\"name\":\"blocksize\",\"type\":[\"int\",\"null\"]},{\"name\":\"inMemory\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"timeToLive\",\"type\":[\"int\",\"null\"]},{\"name\":\"blockCacheEnabled\",\"type\":[\"boolean\",\"null\"]}]},{\"type\":\"record\",\"name\":\"ATableDescriptor\",\"fields\":[{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"families\",\"type\":[{\"type\":\"array\",\"items\":\"AFamilyDescriptor\"},\"null\"]},{\"name\":\"maxFileSize\",\"type\":[\"long\",\"null\"]},{\"name\":\"memStoreFlushSize\",\"type\":[\"long\",\"null\"]},{\"name\":\"rootRegion\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"metaRegion\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"metaTable\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"readOnly\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"deferredLogFlush\",\"type\":[\"boolean\",\"null\"]}]},{\"type\":\"record\",\"name\":\"AColumn\",\"fields\":[{\"name\":\"family\",\"type\":\"bytes\"},{\"name\":\"qualifier\",\"type\":[\"bytes\",\"null\"]}]},{\"type\":\"record\",\"name\":\"AResultEntry\",\"fields\":[{\"name\":\"family\",\"type\":\"bytes\"},{\"name\":\"qualifier\",\"type\":\"bytes\"},{\"name\":\"value\",\"type\":\"bytes\"},{\"name\":\"timestamp\",\"type\":\"long\"}]},{\"type\":\"record\",\"name\":\"AResult\",\"fields\":[{\"name\":\"row\",\"type\":\"bytes\"},{\"name\":\"entries\",\"type\":[{\"type\":\"array\",\"items\":\"AResultEntry\"},\"null\"]}]},{\"type\":\"record\",\"name\":\"ATimeRange\",\"fields\":[{\"name\":\"minStamp\",\"type\":\"long\"},{\"name\":\"maxStamp\",\"type\":\"long\"}]},{\"type\":\"record\",\"name\":\"AGet\",\"fields\":[{\"name\":\"row\",\"type\":\"bytes\"},{\"name\":\"columns\",\"type\":[{\"type\":\"array\",\"items\":\"AColumn\"},\"null\"]},{\"name\":\"timestamp\",\"type\":[\"long\",\"null\"]},{\"name\":\"timerange\",\"type\":[\"ATimeRange\",\"null\"]},{\"name\":\"maxVersions\",\"type\":[\"int\",\"null\"]}]},{\"type\":\"record\",\"name\":\"AColumnValue\",\"fields\":[{\"name\":\"family\",\"type\":\"bytes\"},{\"name\":\"qualifier\",\"type\":\"bytes\"},{\"name\":\"value\",\"type\":\"bytes\"},{\"name\":\"timestamp\",\"type\":[\"long\",\"null\"]}]},{\"type\":\"record\",\"name\":\"APut\",\"fields\":[{\"name\":\"row\",\"type\":\"bytes\"},{\"name\":\"columnValues\",\"type\":{\"type\":\"array\",\"items\":\"AColumnValue\"}}]},{\"type\":\"record\",\"name\":\"AScan\",\"fields\":[{\"name\":\"startRow\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"stopRow\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"columns\",\"type\":[{\"type\":\"array\",\"items\":\"AColumn\"},\"null\"]},{\"name\":\"timestamp\",\"type\":[\"long\",\"null\"]},{\"name\":\"timerange\",\"type\":[\"ATimeRange\",\"null\"]},{\"name\":\"maxVersions\",\"type\":[\"int\",\"null\"]}]},{\"type\":\"record\",\"name\":\"ADelete\",\"fields\":[{\"name\":\"row\",\"type\":\"bytes\"},{\"name\":\"columns\",\"type\":[{\"type\":\"array\",\"items\":\"AColumn\"},\"null\"]}]},{\"type\":\"error\",\"name\":\"AIOError\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]},{\"type\":\"error\",\"name\":\"AIllegalArgument\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]},{\"type\":\"error\",\"name\":\"ATableExists\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]},{\"type\":\"error\",\"name\":\"AMasterNotRunning\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}],\"messages\":{\"createTable\":{\"request\":[{\"name\":\"table\",\"type\":\"ATableDescriptor\"}],\"response\":\"null\",\"errors\":[\"AIOError\",\"AIllegalArgument\",\"ATableExists\",\"AMasterNotRunning\"]},\"enableTable\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"}],\"response\":\"null\",\"errors\":[\"AIOError\"]},\"disableTable\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"}],\"response\":\"null\",\"errors\":[\"AIOError\"]},\"isTableEnabled\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"}],\"response\":\"boolean\",\"errors\":[\"AIOError\"]},\"tableExists\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"}],\"response\":\"boolean\",\"errors\":[\"AIOError\"]},\"listTables\":{\"request\":[],\"response\":{\"type\":\"array\",\"items\":\"ATableDescriptor\"},\"errors\":[\"AIOError\"]},\"describeTable\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"}],\"response\":\"ATableDescriptor\",\"errors\":[\"AIOError\"]},\"describeFamily\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"},{\"name\":\"family\",\"type\":\"bytes\"}],\"response\":\"AFamilyDescriptor\",\"errors\":[\"AIOError\"]},\"get\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"},{\"name\":\"get\",\"type\":\"AGet\"}],\"response\":\"AResult\",\"errors\":[\"AIOError\"]},\"put\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"},{\"name\":\"put\",\"type\":\"APut\"}],\"response\":\"null\",\"errors\":[\"AIOError\"]},\"scannerOpen\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"},{\"name\":\"scan\",\"type\":\"AScan\"}],\"response\":\"int\",\"errors\":[\"AIOError\"]},\"scannerClose\":{\"request\":[{\"name\":\"scannerId\",\"type\":\"int\"}],\"response\":\"null\",\"errors\":[\"AIOError\",\"AIllegalArgument\"]},\"scannerGetRows\":{\"request\":[{\"name\":\"scannerId\",\"type\":\"int\"},{\"name\":\"numberOfRows\",\"type\":\"int\"}],\"response\":{\"type\":\"array\",\"items\":\"AResult\"},\"errors\":[\"AIOError\",\"AIllegalArgument\"]},\"delete\":{\"request\":[{\"name\":\"table\",\"type\":\"bytes\"},{\"name\":\"delete\",\"type\":\"ADelete\"}],\"response\":\"null\",\"errors\":[\"AIOError\"]}}}");
  java.lang.Void createTable(org.apache.hadoop.hbase.avro.generated.ATableDescriptor table)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError, org.apache.hadoop.hbase.avro.generated.AIllegalArgument, org.apache.hadoop.hbase.avro.generated.ATableExists, org.apache.hadoop.hbase.avro.generated.AMasterNotRunning;
  java.lang.Void enableTable(java.nio.ByteBuffer table)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  java.lang.Void disableTable(java.nio.ByteBuffer table)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  boolean isTableEnabled(java.nio.ByteBuffer table)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  boolean tableExists(java.nio.ByteBuffer table)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.ATableDescriptor> listTables()
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  org.apache.hadoop.hbase.avro.generated.ATableDescriptor describeTable(java.nio.ByteBuffer table)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  org.apache.hadoop.hbase.avro.generated.AFamilyDescriptor describeFamily(java.nio.ByteBuffer table, java.nio.ByteBuffer family)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  org.apache.hadoop.hbase.avro.generated.AResult get(java.nio.ByteBuffer table, org.apache.hadoop.hbase.avro.generated.AGet get)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  java.lang.Void put(java.nio.ByteBuffer table, org.apache.hadoop.hbase.avro.generated.APut put)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  int scannerOpen(java.nio.ByteBuffer table, org.apache.hadoop.hbase.avro.generated.AScan scan)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
  java.lang.Void scannerClose(int scannerId)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError, org.apache.hadoop.hbase.avro.generated.AIllegalArgument;
  org.apache.avro.generic.GenericArray<org.apache.hadoop.hbase.avro.generated.AResult> scannerGetRows(int scannerId, int numberOfRows)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError, org.apache.hadoop.hbase.avro.generated.AIllegalArgument;
  java.lang.Void delete(java.nio.ByteBuffer table, org.apache.hadoop.hbase.avro.generated.ADelete delete)
    throws org.apache.avro.ipc.AvroRemoteException, org.apache.hadoop.hbase.avro.generated.AIOError;
}
