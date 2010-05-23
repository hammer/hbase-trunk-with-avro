In this fork I hope to define and Avro "gateway" [interface](http://github.com/hammer/hbase-trunk-with-avro/blob/trunk/core/src/main/java/org/apache/hadoop/hbase/avro/hbase.genavro) to HBase as well as provide a Java service [implementation](http://github.com/hammer/hbase-trunk-with-avro/blob/trunk/core/src/main/java/org/apache/hadoop/hbase/avro/AvroServer.java).

I'd like the implementation to use the new Get/Put/Delete/Scan APIs available in trunk. The scope of the work encompasses both [HBASE-2400](https://issues.apache.org/jira/browse/HBASE-2400) and [HBASE-1744](https://issues.apache.org/jira/browse/HBASE-1744).

To use, first we compile and start HBase and the Avro server:

    $ git clone git@github.com:hammer/hbase-trunk-with-avro.git hbase-trunk-with-avro
    $ cd hbase-trunk-with-avro
    $ export HBASE_HOME=${PWD}
    $ mvn -DskipTests install
    $ bin/start-hbase.sh
    $ bin/hbase avro start

In another terminal, we download the Avro Python client and manipulate our HBase installation:

    $ sudo pip install pyhbase
    $ pyhbase-cli create_table t1 cf1
    $ pyhbase-cli describe_table t1
    $ pyhbase-cli put t1 r1 cf1:c1 fromavro
    $ pyhbase-cli get t1 r1
