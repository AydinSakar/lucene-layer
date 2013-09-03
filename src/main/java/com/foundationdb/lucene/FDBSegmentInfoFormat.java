package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.codecs.SegmentInfoWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FDBSegmentInfoFormat extends SegmentInfoFormat
{
    private static final String SI_EXTENSION = "si";
    private static final String VERSION = "version";
    private static final String DOC_COUNT = "doc_count";
    private static final String COMPOUND_FILE = "compound_file";
    private static final String DIAGNOSTICS = "diagnostics";
    private static final String ATTRIBUTES = "attributes ";
    private static final String FILES = "files";


    //
    // SegmentInfoFormat
    //

    @Override
    public SegmentInfoReader getSegmentInfoReader() {
        return new FDBSegmentInfoReader();
    }

    @Override
    public SegmentInfoWriter getSegmentInfoWriter() {
        return new FDBSegmentInfoWriter();
    }


    //
    // SegmentInfoReader
    //

    private static class FDBSegmentInfoReader extends SegmentInfoReader
    {
        @Override
        public SegmentInfo read(Directory dirIn, String segmentName, IOContext context) throws IOException {
            FDBDirectory dir = Util.unwrapDirectory(dirIn);
            Transaction txn = dir.txn;
            Tuple segmentTuple = dir.subspace.add(segmentName).add(SI_EXTENSION);

            String version = null;
            Integer docCount = null;
            Boolean isCompoundFile = null;
            Map<String, String> diagnostics = new HashMap<String, String>();
            Map<String, String> attributes = new HashMap<String, String>();
            Set<String> files = new HashSet<String>();

            for(KeyValue kv : txn.getRange(segmentTuple.range())) {
                Tuple keyTuple = Tuple.fromBytes(kv.getKey());
                Tuple valueTuple = Tuple.fromBytes(kv.getValue());
                String key = keyTuple.getString(segmentTuple.size());
                boolean found = true;
                if(keyTuple.size() == (segmentTuple.size() + 1)) {
                    if(VERSION.equals(key)) {
                        version = valueTuple.getString(0);
                    } else if(DOC_COUNT.equals(key)) {
                        docCount = (int)valueTuple.getLong(0);
                    } else if(COMPOUND_FILE.equals(key)) {
                        isCompoundFile = valueTuple.getLong(0) == 1;
                    } else {
                        found = false;
                    }
                } else if(keyTuple.size() == (segmentTuple.size() + 2)) {
                    if(DIAGNOSTICS.equals(key)) {
                        diagnostics.put(keyTuple.getString(segmentTuple.size() + 1), valueTuple.getString(0));
                    } else if(ATTRIBUTES.equals(key)) {
                        attributes.put(keyTuple.getString(segmentTuple.size() + 1), valueTuple.getString(0));
                    } else if(FILES.equals(key)) {
                        files.add(keyTuple.getString(segmentTuple.size() + 1));
                    } else {
                        found = false;
                    }
                } else {
                    found = false;
                }
                if(!found) {
                    throw new IllegalStateException("Unexpected key: " + key);
                }
            }

            required(segmentName, VERSION, version);
            required(segmentName, DOC_COUNT, docCount);
            required(segmentName, COMPOUND_FILE, isCompoundFile);

            SegmentInfo info = new SegmentInfo(
                    dirIn,
                    version,
                    segmentName,
                    docCount,
                    isCompoundFile,
                    null,
                    diagnostics,
                    Collections.unmodifiableMap(attributes)
            );
            info.setFiles(files);

            return info;
        }

        private static void required(String segmentName, String keyName, Object value) {
            if(value == null) {
                throw new IllegalStateException("Segment " + segmentName + " missing key: " + keyName);
            }
        }
    }

    //
    // SegmentInfoWriter
    //

    private static class FDBSegmentInfoWriter extends SegmentInfoWriter
    {
        @Override
        public void write(Directory dirIn, SegmentInfo si, FieldInfos fis, IOContext ioContext) throws IOException {
            FDBDirectory dir = Util.unwrapDirectory(dirIn);
            Transaction txn = dir.txn;

            Tuple segmentTuple = dir.subspace.add(si.name).add(SI_EXTENSION);

            set(txn, segmentTuple, VERSION, si.getVersion());
            set(txn, segmentTuple, DOC_COUNT, si.getDocCount());
            set(txn, segmentTuple, COMPOUND_FILE, si.getUseCompoundFile() ? 1 : 0);

            writeMap(txn, segmentTuple.add(DIAGNOSTICS), si.getDiagnostics());
            writeMap(txn, segmentTuple.add(ATTRIBUTES), si.attributes());

            Set<String> files = si.files();
            if(files != null && !files.isEmpty()) {
                Tuple fileTuple = segmentTuple.add(FILES);
                for(String fileName : files) {
                    set(txn, fileTuple, fileName, null);
                }
            }
        }
    }


    //
    // Helpers
    //

    private static void set(Transaction txn, Tuple baseTuple, String key, Object value) {
        Tuple valueTuple = (value != null) ? Tuple.from(value) : Tuple.from();
        txn.set(baseTuple.add(key).pack(), valueTuple.pack());
    }

    private static void writeMap(Transaction txn, Tuple baseTuple, Map<String,String> map) {
        if(map == null || map.isEmpty()) {
            return;
        }
        for(Map.Entry<String, String> entry : map.entrySet()) {
            set(txn, baseTuple, entry.getKey(), entry.getValue());
        }
    }
}
