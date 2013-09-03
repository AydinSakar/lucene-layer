package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.MutableBits;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;

//
// (livTuple) => (totalSize)
// (livTuple, setBitIndex0) => []
// (livTuple, setBitIndex1) => []
// ...
//
public class FDBLiveDocsFormat extends LiveDocsFormat
{
    private static final String LIVEDOCS_EXTENSION = "liv";
    private static final byte[] EMPTY_BYTES = new byte[0];


    @Override
    public MutableBits newLiveDocs(int size) {
        return new FDBBits(size);
    }

    @Override
    public MutableBits newLiveDocs(Bits existing) {
        final FDBBits bits = (FDBBits)existing;
        return new FDBBits(bits);
    }

    @Override
    public Bits readLiveDocs(Directory directory, SegmentInfoPerCommit info, IOContext context) {
        neverCalled();

        assert info.hasDeletions() : "Has no deletions";

        FDBDirectory dir = Util.unwrapDirectory(directory);
        Tuple livTuple = makeLivTuple(dir, info);

        byte[] sizeBytes = dir.txn.get(livTuple.pack()).get();
        assert sizeBytes != null : "No such livTuple";

        int totalSize = (int)Tuple.fromBytes(sizeBytes).getLong(0);
        BitSet bits = new BitSet(totalSize);
        for(KeyValue kv : dir.txn.getRange(livTuple.range())) {
            int i = (int)Tuple.fromBytes(kv.getKey()).getLong(livTuple.size());
            bits.set(i);
        }
        return new FDBBits(bits, totalSize);
    }

    @Override
    public void writeLiveDocs(MutableBits liveDocs, Directory directory, SegmentInfoPerCommit info, int newDelCount, IOContext context) {
        neverCalled();

        FDBDirectory dir = Util.unwrapDirectory(directory);
        Tuple livTuple = makeLivTuple(dir, info);

        FDBBits bits = (FDBBits)liveDocs;
        dir.txn.set(livTuple.pack(), Tuple.from(bits.size).pack());

        for(int i = bits.bitSet.nextSetBit(0); i >= 0; i = bits.bitSet.nextSetBit(i + 1)) {
            dir.txn.set(livTuple.add(i).pack(), EMPTY_BYTES);
        }
    }

    @Override
    public void files(SegmentInfoPerCommit info, Collection<String> files) throws IOException {
        // TODO: Needed?
        //if(info.hasDeletions()) {
        //    files.add(IndexFileNames.fileNameFromGeneration(info.info.name, LIVEDOCS_EXTENSION, info.getDelGen()));
        //}`
    }


    //
    // Helpers
    //

    private static Tuple makeLivTuple(FDBDirectory dir, SegmentInfoPerCommit info) {
        return dir.subspace.add(info.info.name).add(LIVEDOCS_EXTENSION).add(info.getNextDelGen());
    }

    // Until a test is found that exercises this;
    private static void neverCalled() {
        throw new IllegalStateException("Never called");
    }

    private static class FDBBits implements Bits, MutableBits
    {
        final int size;
        final BitSet bitSet;

        public FDBBits(int size) {
            this(new BitSet(size), size);
        }

        public FDBBits(BitSet bitSet, int size) {
            this.bitSet = bitSet;
            this.size = size;
        }

        public FDBBits(FDBBits bits) {
            this.bitSet = (BitSet)bits.bitSet.clone();
            this.size = bits.size;
        }

        @Override
        public boolean get(int index) {
            return bitSet.get(index);
        }

        @Override
        public int length() {
            return size;
        }

        @Override
        public void clear(int bit) {
            bitSet.clear(bit);
        }
    }
}
