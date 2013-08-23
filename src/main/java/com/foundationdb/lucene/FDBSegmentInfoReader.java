package com.foundationdb.lucene;

import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_ATT_KEY;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_ATT_VALUE;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_DIAG_KEY;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_DIAG_VALUE;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_DOCCOUNT;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_FILE;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_NUM_ATTS;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_NUM_DIAG;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_NUM_FILES;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_USECOMPOUND;
import static com.foundationdb.lucene.FDBSegmentInfoWriter.SI_VERSION;

public class FDBSegmentInfoReader extends SegmentInfoReader
{

    @Override
    public SegmentInfo read(Directory directory, String segmentName, IOContext context) throws IOException {
        BytesRef scratch = new BytesRef();
        String segFileName = IndexFileNames.segmentFileName(segmentName, "", FDBSegmentInfoFormat.SI_EXTENSION);
        IndexInput input = directory.openInput(segFileName, context);
        boolean success = false;
        try {
            FDBUtil.readLine(input, scratch);
            assert StringHelper.startsWith(scratch, SI_VERSION);
            final String version = readString(SI_VERSION.length, scratch);

            FDBUtil.readLine(input, scratch);
            assert StringHelper.startsWith(scratch, SI_DOCCOUNT);
            final int docCount = Integer.parseInt(readString(SI_DOCCOUNT.length, scratch));

            FDBUtil.readLine(input, scratch);
            assert StringHelper.startsWith(scratch, SI_USECOMPOUND);
            final boolean isCompoundFile = Boolean.parseBoolean(readString(SI_USECOMPOUND.length, scratch));

            FDBUtil.readLine(input, scratch);
            assert StringHelper.startsWith(scratch, SI_NUM_DIAG);
            int numDiag = Integer.parseInt(readString(SI_NUM_DIAG.length, scratch));
            Map<String, String> diagnostics = new HashMap<String, String>();

            for(int i = 0; i < numDiag; i++) {
                FDBUtil.readLine(input, scratch);
                assert StringHelper.startsWith(scratch, SI_DIAG_KEY);
                String key = readString(SI_DIAG_KEY.length, scratch);

                FDBUtil.readLine(input, scratch);
                assert StringHelper.startsWith(scratch, SI_DIAG_VALUE);
                String value = readString(SI_DIAG_VALUE.length, scratch);
                diagnostics.put(key, value);
            }

            FDBUtil.readLine(input, scratch);
            assert StringHelper.startsWith(scratch, SI_NUM_ATTS);
            int numAtts = Integer.parseInt(readString(SI_NUM_ATTS.length, scratch));
            Map<String, String> attributes = new HashMap<String, String>();

            for(int i = 0; i < numAtts; i++) {
                FDBUtil.readLine(input, scratch);
                assert StringHelper.startsWith(scratch, SI_ATT_KEY);
                String key = readString(SI_ATT_KEY.length, scratch);

                FDBUtil.readLine(input, scratch);
                assert StringHelper.startsWith(scratch, SI_ATT_VALUE);
                String value = readString(SI_ATT_VALUE.length, scratch);
                attributes.put(key, value);
            }

            FDBUtil.readLine(input, scratch);
            assert StringHelper.startsWith(scratch, SI_NUM_FILES);
            int numFiles = Integer.parseInt(readString(SI_NUM_FILES.length, scratch));
            Set<String> files = new HashSet<String>();

            for(int i = 0; i < numFiles; i++) {
                FDBUtil.readLine(input, scratch);
                assert StringHelper.startsWith(scratch, SI_FILE);
                String fileName = readString(SI_FILE.length, scratch);
                files.add(fileName);
            }

            SegmentInfo info = new SegmentInfo(
                    directory,
                    version,
                    segmentName,
                    docCount,
                    isCompoundFile,
                    null,
                    diagnostics,
                    Collections.unmodifiableMap(attributes)
            );
            info.setFiles(files);
            success = true;
            return info;
        } finally {
            if(!success) {
                IOUtils.closeWhileHandlingException(input);
            } else {
                input.close();
            }
        }
    }

    private String readString(int offset, BytesRef scratch) {
        return new String(scratch.bytes, scratch.offset + offset, scratch.length - offset, IOUtils.CHARSET_UTF_8);
    }
}
