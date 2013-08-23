package com.foundationdb.lucene;

import org.apache.lucene.codecs.SegmentInfoWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class FDBSegmentInfoWriter extends SegmentInfoWriter
{

    final static BytesRef SI_VERSION = new BytesRef("    version ");
    final static BytesRef SI_DOCCOUNT = new BytesRef("    number of documents ");
    final static BytesRef SI_USECOMPOUND = new BytesRef("    uses compound file ");
    final static BytesRef SI_NUM_DIAG = new BytesRef("    diagnostics ");
    final static BytesRef SI_DIAG_KEY = new BytesRef("      key ");
    final static BytesRef SI_DIAG_VALUE = new BytesRef("      value ");
    final static BytesRef SI_NUM_ATTS = new BytesRef("    attributes ");
    final static BytesRef SI_ATT_KEY = new BytesRef("      key ");
    final static BytesRef SI_ATT_VALUE = new BytesRef("      value ");
    final static BytesRef SI_NUM_FILES = new BytesRef("    files ");
    final static BytesRef SI_FILE = new BytesRef("      file ");

    @Override
    public void write(Directory dir, SegmentInfo si, FieldInfos fis, IOContext ioContext) throws IOException {

        String segFileName = IndexFileNames.segmentFileName(si.name, "", FDBSegmentInfoFormat.SI_EXTENSION);
        si.addFile(segFileName);

        boolean success = false;
        IndexOutput output = dir.createOutput(segFileName, ioContext);

        try {
            BytesRef scratch = new BytesRef();

            FDBUtil.write(output, SI_VERSION);
            FDBUtil.write(output, si.getVersion(), scratch);
            FDBUtil.writeNewline(output);

            FDBUtil.write(output, SI_DOCCOUNT);
            FDBUtil.write(output, Integer.toString(si.getDocCount()), scratch);
            FDBUtil.writeNewline(output);

            FDBUtil.write(output, SI_USECOMPOUND);
            FDBUtil.write(output, Boolean.toString(si.getUseCompoundFile()), scratch);
            FDBUtil.writeNewline(output);

            Map<String, String> diagnostics = si.getDiagnostics();
            int numDiagnostics = diagnostics == null ? 0 : diagnostics.size();
            FDBUtil.write(output, SI_NUM_DIAG);
            FDBUtil.write(output, Integer.toString(numDiagnostics), scratch);
            FDBUtil.writeNewline(output);

            if(numDiagnostics > 0) {
                for(Map.Entry<String, String> diagEntry : diagnostics.entrySet()) {
                    FDBUtil.write(output, SI_DIAG_KEY);
                    FDBUtil.write(output, diagEntry.getKey(), scratch);
                    FDBUtil.writeNewline(output);

                    FDBUtil.write(output, SI_DIAG_VALUE);
                    FDBUtil.write(output, diagEntry.getValue(), scratch);
                    FDBUtil.writeNewline(output);
                }
            }

            Map<String, String> atts = si.attributes();
            int numAtts = atts == null ? 0 : atts.size();
            FDBUtil.write(output, SI_NUM_ATTS);
            FDBUtil.write(output, Integer.toString(numAtts), scratch);
            FDBUtil.writeNewline(output);

            if(numAtts > 0) {
                for(Map.Entry<String, String> entry : atts.entrySet()) {
                    FDBUtil.write(output, SI_ATT_KEY);
                    FDBUtil.write(output, entry.getKey(), scratch);
                    FDBUtil.writeNewline(output);

                    FDBUtil.write(output, SI_ATT_VALUE);
                    FDBUtil.write(output, entry.getValue(), scratch);
                    FDBUtil.writeNewline(output);
                }
            }

            Set<String> files = si.files();
            int numFiles = files == null ? 0 : files.size();
            FDBUtil.write(output, SI_NUM_FILES);
            FDBUtil.write(output, Integer.toString(numFiles), scratch);
            FDBUtil.writeNewline(output);

            if(numFiles > 0) {
                for(String fileName : files) {
                    FDBUtil.write(output, SI_FILE);
                    FDBUtil.write(output, fileName, scratch);
                    FDBUtil.writeNewline(output);
                }
            }
            success = true;
        } finally {
            if(!success) {
                IOUtils.closeWhileHandlingException(output);
                try {
                    dir.deleteFile(segFileName);
                } catch(Throwable t) {
                }
            } else {
                output.close();
            }
        }
    }
}
