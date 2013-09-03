package com.foundationdb.lucene;

import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FDBDirectory extends Directory
{
    /** See {@link RAMInputStream#BUFFER_SIZE} */
    private static final int BUFFER_SIZE = 1024;

    public Transaction txn;
    public final Tuple subspace;
    private final Tuple dirSubspace;
    private final Tuple dataSubspace;


    public FDBDirectory(String path, Transaction txn) {
        this(Tuple.from(Util.DEFAULT_ROOT_PREFIX, path), txn);
    }

    public FDBDirectory(Tuple subspace, Transaction txn) {
        this(subspace, txn, NoLockFactory.getNoLockFactory());
    }

    FDBDirectory(Tuple subspace, Transaction txn, LockFactory lockFactory) {
        assert subspace != null;
        assert txn != null;
        assert lockFactory != null;
        this.txn = txn;
        this.subspace = subspace;
        this.dirSubspace = subspace.add(0);
        this.dataSubspace = subspace.add(1);
        try {
            setLockFactory(lockFactory);
        } catch(IOException e) {
            throw new IllegalStateException("setLockFactory threw", e);
        }
    }

    private class Output extends RAMOutputStream
    {
        private final String name;
        private final long dataID;
        private boolean doingFlush = false;

        public Output(String name, long dataID) {
            this.name = name;
            this.dataID = dataID;
        }

        @Override
        public void flush() {
            if(doingFlush) {
                return;
            }
            doingFlush = true;
            try {
                flushInternal();
            } finally {
                doingFlush = false;
            }
        }

        private void flushInternal() {
            try {
                // Sets file length
                super.flush();
                byte[] outValue = new byte[(int)length()];
                writeTo(outValue, 0);
                Util.writeLargeValue(txn, dataSubspace.add(dataID), BUFFER_SIZE, outValue);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            flush();
        }
    }

    private static class InputFile extends RAMFile
    {
        public byte[] addBufferInternal(int size) {
            return super.addBuffer(size);
        }

        public void setLengthInternal(int length) {
            super.setLength(length);
        }
    }


    //
    // Directory
    //

    private long getDataID(String name) {
        byte[] value = txn.get(dirSubspace.add(name).pack()).get();
        if(value == null) {
            return -1;
        }
        return Tuple.fromBytes(value).getLong(0);
    }

    private long createDataID(String name) {
        AsyncIterator<KeyValue> it = txn.getRange(dataSubspace.range(), 1, true).iterator();
        long nextID = 0;
        if(it.hasNext()) {
            KeyValue kv = it.next();
            nextID = Tuple.fromBytes(kv.getKey()).getLong(dataSubspace.size()) + 1;
        }
        txn.set(dirSubspace.add(name).pack(), Tuple.from(nextID).pack());
        txn.set(dataSubspace.add(nextID).add(0).pack(), new byte[0]);
        return nextID;
    }

    @Override
    public String[] listAll() {
        List<String> outList = new ArrayList<String>();
        for(KeyValue kv : txn.getRange(dirSubspace.range())) {
            outList.add(Tuple.fromBytes(kv.getKey()).getString(dirSubspace.size()));
        }
        return outList.toArray(new String[outList.size()]);
    }

    @Override
    public boolean fileExists(String name) {
        Util.specialFileExists(name, this);
        return getDataID(name) != -1;
    }

    @Override
    public void deleteFile(String name) throws NoSuchFileException {
        long dataID = getDataID(name);
        if(dataID == -1) {
            throw new NoSuchFileException(name);
        }
        txn.clear(dirSubspace.add(name).pack());
        txn.clear(dataSubspace.add(dataID).range());
    }

    @Override
    public long fileLength(String name) throws IOException {
        return openInput(name, null).length();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws FileAlreadyExistsException {
        if(getDataID(name) != -1) {
            throw new FileAlreadyExistsException(name);
        }
        return new Output(name, createDataID(name));
    }

    @Override
    public void sync(Collection<String> names) {
        // None
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        long dataID = getDataID(name);
        if(dataID == -1) {
            throw new FileNotFoundException(name);
        }
        InputFile file = new InputFile();
        int totalLen = 0;
        for(KeyValue kv : txn.getRange(dataSubspace.add(dataID).range())) {
            byte[] value = kv.getValue();
            byte[] ramValue = file.addBufferInternal(value.length);
            totalLen += value.length;
            System.arraycopy(value, 0, ramValue, 0, value.length);
        }
        file.setLengthInternal(totalLen);
        return new RAMInputStream(name, file);
    }

    @Override
    public void close() {
        // None
    }
}