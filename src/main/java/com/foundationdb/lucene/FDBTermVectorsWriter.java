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

    private final FDBDirectory dir;
    private final Tuple segmentTuple;
    private int numDocsWritten;
    private int numTermsWritten;

    private Tuple docTuple;
    private Tuple fieldTuple;
    private Tuple termTuple;

    private boolean hasPositions;
    private boolean hasOffsets;
    private boolean hasPayloads;


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
    // (segmentName, "vec", docNum, fieldNum) => (name, hasPositions, hasOffsets, hasPayloads)
    // (segmentName, "vec", docNum, fieldNum, term0) => (termBytes, freq)
    // (segmentName, "vec", docNum, fieldNum, term0, posNum0) => (start_offset, end_offset, payload)
    // (segmentName, "vec", docNum, fieldNum, term0, posNum1) => (start_offset, end_offset, payload)
    // (segmentName, "vec", docNum, fieldNum, term1) => (termBytes, freq)
    // ...
    //

    @Override
    public void startField(FieldInfo info, int numTerms, boolean positions, boolean offsets, boolean payloads) {
        fieldTuple = docTuple.add(info.number);
        Transaction txn = dir.txn;

        txn.set(fieldTuple.pack(), Tuple.from(info.name, positions ? 1 : 0, offsets ? 1 : 0, payloads ? 1 : 0).pack());

        hasPositions = positions;
        hasOffsets = offsets;
        hasPayloads = payloads;
        numTermsWritten = 0;
    }

    @Override
    public void startTerm(BytesRef term, int freq) throws IOException {
        termTuple = fieldTuple.add(numTermsWritten);
        dir.txn.set(termTuple.pack(), Tuple.from(FDBDirectory.copyRange(term), freq).pack());
        ++numTermsWritten;
    }

    @Override
    public void addPosition(int position, int startOffset, int endOffset, BytesRef payload) {
        Transaction txn = dir.txn;
        Tuple valueTuple = Tuple.from(
                hasOffsets ? startOffset : null,
                hasOffsets ? endOffset : null,
                hasPayloads ? FDBDirectory.copyRange(payload) : null
        );
        txn.set(termTuple.add(position).pack(), valueTuple.pack());
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
