package com.foundationdb.lucene;

import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collection;

public class FDBTestDirectory extends FSDirectory
{
    private final FDBDirectory fdbDir;
    private FDBTestDirectory subDir;
    private int subDirCount;
    private boolean closed;


    public FDBTestDirectory(File path) throws IOException {
        this(path, Util.test_CreateTransaction());
        // Note: Not ideal, as 'this' escapes, but fully constructed at this point
        Util.test_SetDirectory(this);
    }

    private FDBTestDirectory(File path, Transaction txn) throws IOException {
        this(path, txn, NoLockFactory.getNoLockFactory());
    }

    private FDBTestDirectory(File path, Transaction txn, LockFactory lockFactory) throws IOException {
        super(path, lockFactory);
        assert txn != null;
        Tuple subspace = Tuple.from(Util.DEFAULT_TEST_ROOT_PREFIX, path.getAbsolutePath());
        this.fdbDir = new FDBDirectory(subspace, txn, lockFactory);
    }

    public FDBDirectory getFDBDirectory() {
        return fdbDir;
    }

    synchronized FDBTestDirectory getSubDir() {
        if(subDir == null || subDir.closed) {
            File path = new File(getDirectory(), "SubDir_" + subDirCount++);
            try {
                subDir = new FDBTestDirectory(path, Util.test_CreateTransaction());
            } catch(IOException e) {
                throw new IllegalStateException("Constructor threw", e);
            }
        }
        return subDir;
    }


    //
    // Directory
    //

    @Override
    public String[] listAll() {
        return fdbDir.listAll();

    }

    @Override
    public boolean fileExists(String name) {
        return fdbDir.fileExists(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        fdbDir.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return fdbDir.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws FileAlreadyExistsException {
        return fdbDir.createOutput(name, context);
    }

    @Override
    public void sync(Collection<String> names) {
        fdbDir.sync(names);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return fdbDir.openInput(name, context);
    }

    @Override
    public void close() {
        if(subDir != null) {
            subDir.close();
        }
        fdbDir.close();
        Util.test_ClearDirectory(this);
        closed = true;
    }
}
