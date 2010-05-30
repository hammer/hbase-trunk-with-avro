package org.apache.hadoop.hbase.avro.generated;

@SuppressWarnings("all")
public class AServerAddress extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AServerAddress\",\"namespace\":\"org.apache.hadoop.hbase.avro.generated\",\"fields\":[{\"name\":\"bindAddress\",\"type\":\"string\"},{\"name\":\"hostname\",\"type\":\"string\"},{\"name\":\"inetSocketAddress\",\"type\":\"string\"},{\"name\":\"port\",\"type\":\"int\"}]}");
  public org.apache.avro.util.Utf8 bindAddress;
  public org.apache.avro.util.Utf8 hostname;
  public org.apache.avro.util.Utf8 inetSocketAddress;
  public int port;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return bindAddress;
    case 1: return hostname;
    case 2: return inetSocketAddress;
    case 3: return port;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: bindAddress = (org.apache.avro.util.Utf8)value$; break;
    case 1: hostname = (org.apache.avro.util.Utf8)value$; break;
    case 2: inetSocketAddress = (org.apache.avro.util.Utf8)value$; break;
    case 3: port = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
