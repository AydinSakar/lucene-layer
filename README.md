# FoundationDB Lucene Layer

## Running Tests

`mvn test`

## Building

`mvn package`

## Running Lucene and Solr Tests

`ant test -DFDBCodec.test_mode=true -Dtests.directory=com.foundationdb.lucene.FDBTestDirectory -Dtests.codec=FDBCodec -lib fdb-lucene-1.0.0-SNAPSHOT.jar`
