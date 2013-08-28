package com.foundationdb.lucene;

import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;

import java.util.Comparator;

public abstract class FDBTermsBase extends Terms
{
    private final boolean hasOffsets;
    private final boolean hasPositions;
    private final boolean hasPayloads;
    private final long size;
    private final long sumDocFreq;
    private final int docCount;

    public FDBTermsBase(boolean hasOffsets,
                        boolean hasPositions,
                        boolean hasPayloads,
                        long size,
                        long sumDocFeq,
                        int docCount) {
        this.hasOffsets = hasOffsets;
        this.hasPositions = hasPositions;
        this.hasPayloads = hasPayloads;
        this.size = size;
        this.sumDocFreq = sumDocFeq;
        this.docCount = docCount;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long getSumTotalTermFreq() {
        return -1;
    }

    @Override
    public long getSumDocFreq() {
        return sumDocFreq;
    }

    @Override
    public int getDocCount() {
        return docCount;
    }

    @Override
    public boolean hasOffsets() {
        return hasOffsets;
    }

    @Override
    public boolean hasPositions() {
        return hasPositions;
    }

    @Override
    public boolean hasPayloads() {
        return hasPayloads;
    }
}
