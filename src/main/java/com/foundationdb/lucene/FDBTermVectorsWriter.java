package com.foundationdb.lucene;

import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;

public class FDBTermVectorsWriter extends TermVectorsWriter
{
    static final String VECTORS_EXTENSION = "vec";
    static final String FIELD_NAME = "name";
    static final String FIELD_POSITIONS = "positions";
    static final String FIELD_OFFSETS = "offsets";
    static final String FIELD_PAYLOADS = "payloads";
    static final String TERM = "term";
    static final String PAYLOAD = "payload";
    static final String START_OFFSET = "startoffset";
    static final String END_OFFSET = "endoffset";


    private final FDBDirectory dir;
    private final Tuple segmentTuple;
    private int numDocsWritten;
    private int numTermsWritten;

    private Tuple docTuple;
    private Tuple fieldTuple;
    private Tuple termTuple;

    private boolean withPositions;
    private boolean withOffsets;
    private boolean withPayloads;


    public FDBTermVectorsWriter(Directory dirIn, String segmentName) {
        this.dir = FDBDirectory.unwrapFDBDirectory(dirIn);
        this.segmentTuple = dir.subspace.add(segmentName).add(VECTORS_EXTENSION);
    }

    @Override
    public void startDocument(int numVectorFields) {
        docTuple = segmentTuple.add(numDocsWritten);
        ++numDocsWritten;
    }


    //
    // (segmentName, "vec", docNum, fieldNum, "name") => name
    // (segmentName, "vec", docNum, fieldNum, "positions") => [0|1]
    // (segmentName, "vec", docNum, fieldNum, "offets") => [0|1]
    // (segmentName, "vec", docNum, fieldNum, "payloads") => [0|1]
    // (segmentName, "vec", docNum, fieldNum, "term", 0) => (termBytes, freq)
    // (segmentName, "vec", docNum, fieldNum, "term", 0, posNum) => ()
    // (segmentName, "vec", docNum, fieldNum, "term", 0, posNum, "payload") => [payload]
    // (segmentName, "vec", docNum, fieldNum, "term", 0, posNum, "startoffset") => startOffset
    // (segmentName, "vec", docNum, fieldNum, "term", 0, posNum, "endOffset") => endOffset
    // (segmentName, "vec", docNum, fieldNum, "term", 1) => (termBytes, freq)
    //

    @Override
    public void startField(FieldInfo info, int numTerms, boolean positions, boolean offsets, boolean payloads) {
        fieldTuple = docTuple.add(info.number);
        Transaction txn = dir.txn;

        txn.set(fieldTuple.add(FIELD_NAME).pack(), Tuple.from(info.name).pack());
        txn.set(fieldTuple.add(FIELD_POSITIONS).pack(), Tuple.from(positions ? 1 : 0).pack());
        txn.set(fieldTuple.add(FIELD_OFFSETS).pack(), Tuple.from(offsets ? 1 : 0).pack());
        txn.set(fieldTuple.add(FIELD_PAYLOADS).pack(), Tuple.from(payloads ? 1 : 0).pack());

        withPositions = positions;
        withOffsets = offsets;
        withPayloads = payloads;
        numTermsWritten = 0;
    }

    @Override
    public void startTerm(BytesRef term, int freq) throws IOException {
        termTuple = fieldTuple.add(TERM).add(numTermsWritten);
        dir.txn.set(termTuple.pack(), Tuple.from(FDBDirectory.copyRange(term), freq).pack());
        ++numTermsWritten;
    }

    @Override
    public void addPosition(int position, int startOffset, int endOffset, BytesRef payload) {
        assert withPositions || withOffsets;

        Transaction txn = dir.txn;
        Tuple posTuple = termTuple.add(position);

        if(withPositions) {
            txn.set(posTuple.pack(), Tuple.from().pack());
            if(withPayloads) {
                txn.set(posTuple.add(PAYLOAD).pack(), FDBDirectory.copyRange(payload));
            }
        }

        if(withOffsets) {
            txn.set(posTuple.add(START_OFFSET).pack(), Tuple.from(startOffset).pack());
            txn.set(posTuple.add(END_OFFSET).pack(), Tuple.from(endOffset).pack());
        }
    }

    @Override
    public void abort() {
        // None
    }

    @Override
    public void finish(FieldInfos fis, int numDocs) {
        if(numDocsWritten != numDocs) {
            throw new IllegalStateException("Docs written does not match expected: " + numTermsWritten + " vs " + numDocs);
        }
    }

    @Override
    public void close() {
        // None
    }

    @Override
    public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
}
