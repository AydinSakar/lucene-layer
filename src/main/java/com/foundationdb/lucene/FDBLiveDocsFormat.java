package com.foundationdb.lucene;

import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MutableBits;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;

public class FDBLiveDocsFormat extends LiveDocsFormat
{

    static final String LIVEDOCS_EXTENSION = "liv";

    final static BytesRef SIZE = new BytesRef("size ");
    final static BytesRef DOC = new BytesRef("  doc ");
    final static BytesRef END = new BytesRef("END");

    @Override
    public MutableBits newLiveDocs(int size) throws IOException {
        return new FDBMutableBits(size);
    }

    @Override
    public MutableBits newLiveDocs(Bits existing) throws IOException {
        final FDBBits bits = (FDBBits) existing;
        return new FDBMutableBits((BitSet) bits.bits.clone(), bits.size);
    }

    @Override
    public Bits readLiveDocs(Directory dir, SegmentInfoPerCommit info, IOContext context) throws IOException {
        assert info.hasDeletions();
        BytesRef scratch = new BytesRef();
        CharsRef scratchUTF16 = new CharsRef();

        String fileName = IndexFileNames.fileNameFromGeneration(info.info.name, LIVEDOCS_EXTENSION, info.getDelGen());
        IndexInput in = null;
        boolean success = false;
        try {
            in = dir.openInput(fileName, context);

            FDBUtil.readLine(in, scratch);
            assert StringHelper.startsWith(scratch, SIZE);
            int size = parseIntAt(scratch, SIZE.length, scratchUTF16);

            BitSet bits = new BitSet(size);

            FDBUtil.readLine(in, scratch);
            while(!scratch.equals(END)) {
                assert StringHelper.startsWith(scratch, DOC);
                int docid = parseIntAt(scratch, DOC.length, scratchUTF16);
                bits.set(docid);
                FDBUtil.readLine(in, scratch);
            }

            success = true;
            return new FDBBits(bits, size);
        } finally {
            if(success) {
                IOUtils.close(in);
            } else {
                IOUtils.closeWhileHandlingException(in);
            }
        }
    }

    private int parseIntAt(BytesRef bytes, int offset, CharsRef scratch) {
        UnicodeUtil.UTF8toUTF16(bytes.bytes, bytes.offset + offset, bytes.length - offset, scratch);
        return ArrayUtil.parseInt(scratch.chars, 0, scratch.length);
    }

    @Override
    public void writeLiveDocs(MutableBits bits,
                              Directory dir,
                              SegmentInfoPerCommit info,
                              int newDelCount,
                              IOContext context) throws IOException {
        BitSet set = ((FDBBits) bits).bits;
        int size = bits.length();
        BytesRef scratch = new BytesRef();

        String fileName = IndexFileNames.fileNameFromGeneration(
                info.info.name,
                LIVEDOCS_EXTENSION,
                info.getNextDelGen()
        );
        IndexOutput out = null;
        boolean success = false;
        try {
            out = dir.createOutput(fileName, context);
            FDBUtil.write(out, SIZE);
            FDBUtil.write(out, Integer.toString(size), scratch);
            FDBUtil.writeNewline(out);

            for(int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i + 1)) {
                FDBUtil.write(out, DOC);
                FDBUtil.write(out, Integer.toString(i), scratch);
                FDBUtil.writeNewline(out);
            }

            FDBUtil.write(out, END);
            FDBUtil.writeNewline(out);
            success = true;
        } finally {
            if(success) {
                IOUtils.close(out);
            } else {
                IOUtils.closeWhileHandlingException(out);
            }
        }
    }

    @Override
    public void files(SegmentInfoPerCommit info, Collection<String> files) throws IOException {
        if(info.hasDeletions()) {
            files.add(IndexFileNames.fileNameFromGeneration(info.info.name, LIVEDOCS_EXTENSION, info.getDelGen()));
        }
    }

    // read-only
    static class FDBBits implements Bits
    {
        final BitSet bits;
        final int size;

        FDBBits(BitSet bits, int size) {
            this.bits = bits;
            this.size = size;
        }

        @Override
        public boolean get(int index) {
            return bits.get(index);
        }

        @Override
        public int length() {
            return size;
        }
    }

    // read-write
    static class FDBMutableBits extends FDBBits implements MutableBits
    {

        FDBMutableBits(int size) {
            this(new BitSet(size), size);
            bits.set(0, size);
        }

        FDBMutableBits(BitSet bits, int size) {
            super(bits, size);
        }

        @Override
        public void clear(int bit) {
            bits.clear(bit);
        }
    }
}
