# FoundationDB Lucene Layer


## Running Tests

`$ mvn test`


## Building

`$ mvn package`


## Running Lucene and Solr Tests

1. Package fdb-lucene-layer

        $ mvn package

2. Download the Solr source

        $ curl -O http://mirror.nexcess.net/apache/lucene/solr/4.4.0/solr-4.4.0-src.tgz
        $ tar xzf solr-4.4.0-src.tgz
        $ cd solr-4.4.0/

3. Run the full test suite

        $ ant test -Dtests.codec=FDBCodec \
                   -Dtests.directory=com.foundationdb.lucene.FDBTestDirectory \
                   -lib  ../target/fdb-lucene-layer-0.0.1-SNAPSHOT.jar \
                   -lib ../target/dependency/fdb-java-1.0.0.jar

