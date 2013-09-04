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

import static com.foundationdb.lucene.Util.set;

//
// (livTuple) => (totalSize)
// (livTuple, setBitIndex0) => []
// (livTuple, setBitIndex1) => []
// ...
//
public class FDBLiveDocsFormat extends LiveDocsFormat
{
    private static final String LIVE_DOCS_EXT = "liv";


    @Override
    public MutableBits newLiveDocs(int size) {
        FDBBits bits = new FDBBits(size);
        bits.bitSet.set(0, size);
        return bits;
    }

    @Override
    public MutableBits newLiveDocs(Bits existing) {
        final FDBBits bits = (FDBBits)existing;
        return new FDBBits(bits);
    }

    @Override
    public Bits readLiveDocs(Directory directory, SegmentInfoPerCommit info, IOContext context) {
        assert info.hasDeletions() : "No deletions: " + info.info.name;

        FDBDirectory dir = Util.unwrapDirectory(directory);
        Tuple livTuple = makeLivTuple(dir, info, info.getDelGen());

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
        FDBDirectory dir = Util.unwrapDirectory(directory);
        Tuple livTuple = makeLivTuple(dir, info, info.getNextDelGen());

        FDBBits bits = (FDBBits)liveDocs;
        dir.txn.set(livTuple.pack(), Tuple.from(bits.size).pack());

        for(int i = bits.bitSet.nextSetBit(0); i >= 0; i = bits.bitSet.nextSetBit(i + 1)) {
            set(dir.txn, livTuple, i);
        }
    }

    @Override
    public void files(SegmentInfoPerCommit info, Collection<String> files) throws IOException {
        // TODO: Needed?
        //if(info.hasDeletions()) {
        //    files.add(IndexFileNames.fileNameFromGeneration(info.info.name, LIVE_DOCS_EXT, info.getDelGen()));
        //}`
    }


    //
    // Helpers
    //

    private static Tuple makeLivTuple(FDBDirectory dir, SegmentInfoPerCommit info, long gen) {
        return dir.subspace.add(info.info.name).add(LIVE_DOCS_EXT).add(gen);
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
