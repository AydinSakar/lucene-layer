/**
 * FoundationDB Lucene Layer
 * Copyright (c) 2013 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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

    public FDBTermsBase(boolean hasPositions,
                        boolean hasOffsets,
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
