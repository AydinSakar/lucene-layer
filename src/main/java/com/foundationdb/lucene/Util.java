package com.foundationdb.lucene;

import com.foundationdb.Cluster;
import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Util
{
    public static final int DEFAULT_API_VERSION = 100;
    public static final String DEFAULT_ROOT_PREFIX = "lucene";
    public static final String DEFAULT_TEST_ROOT_PREFIX = "test_" + DEFAULT_ROOT_PREFIX;


    public static FDBDirectory unwrapDirectory(Directory dir) {
        if(dir instanceof FDBDirectory) {
            return (FDBDirectory)dir;
        }
        if(dir instanceof CompoundFileDirectory) {
            return unwrapDirectory(((CompoundFileDirectory)dir).getDirectory());
        }
        Exception cause = null;
        try {
            dir.fileExists(STRING_INSTANCE);
        } catch(DirectoryWrappingException e) {
            return e.getDirectory();
        } catch(IOException e) {
            cause = e;
        }
        FDBTestDirectory threadDir = test_GetDirectory();
        if(threadDir != null) {
            return threadDir.getSubDir().getFDBDirectory();
        }
        throw new IllegalStateException("No FDBDirectory to unwrap", cause);
    }

    /** Write a value in multiple values of, at most, chunkSize. Form keys by appending running total to baseTuple. */
    public static int writeLargeValue(Transaction txn, Tuple baseTuple, int chunkSize, byte[] value) {
        int chunks = 0;
        int bytesWritten = 0;
        while(bytesWritten < value.length) {
            ++chunks;
            int toWrite = Math.min(chunkSize, value.length - bytesWritten);
            txn.set(
                    baseTuple.add(bytesWritten).pack(), Arrays.copyOfRange(value, bytesWritten, bytesWritten + toWrite)
            );
            bytesWritten += toWrite;
        }
        assert bytesWritten == value.length;
        return chunks;
    }

    /** Copy and return bytes that are in use by <code>ref</code>. */
    public static byte[] copyRange(BytesRef ref) {
        if(ref == null) {
            return null;
        }
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
    }

    /** Convert the given little-endian packed byte[8] into a long. */
    public static long unpackLittleEndianLong(byte[] bytes) {
        if(bytes == null) {
            return 0;
        }
        assert bytes.length == 8 : bytes.length;
        long unpacked = 0;
        for(int i = 0; i < 8; ++i) {
            long cur = (bytes[i] & 0xFFL) << (i * 8);
            unpacked += cur;
        }
        return unpacked;
    }

    /** Pretty print the given Tuple */
    public static String tupleString(Tuple t) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        boolean first = true;
        for(Object o : t.getItems()) {
            if(!first) {
                sb.append(",");
            }
            first = false;
            if(o instanceof byte[]) {
                sb.append(ByteArrayUtil.printable((byte[])o));
            } else {
                sb.append(o);
            }
        }
        sb.append(')');
        return sb.toString();
    }


    //
    // Helpers
    //

    private static final String STRING_INSTANCE = new String(new char[0]);

    private static class DirectoryWrappingException extends RuntimeException
    {
        private final FDBDirectory dir;

        public DirectoryWrappingException(FDBDirectory dir) {
            this.dir = dir;
        }

        public FDBDirectory getDirectory() {
            return dir;
        }
    }

    @SuppressWarnings("StringEquality") // Intentional test workaround
    static void specialFileExists(String str, FDBDirectory dir) {
        if(str == STRING_INSTANCE) {
            throw new DirectoryWrappingException(dir);
        }
    }


    //
    // Test Only Helpers
    // Imperfect, but functional, workarounds to allow FDBCodec and FDBTestDirectory to be used in the sock Lucene and
    // Solr test suites. These are hidden behind an opt-in config and prefixed with test_ to indicate as much.
    //

    private static FDB test_FDB = null;
    private static Database test_DB = null;
    private static ThreadLocal<FDBTestDirectory> test_DIR = null;

    private static synchronized void initFDB() {
        if(test_FDB == null) {
            test_FDB = FDB.selectAPIVersion(DEFAULT_API_VERSION);

            // Find system thread group to avoid zombie assertions
            final ThreadGroup threadGroup = findSystemThreadGroup();

            // Create executor for use by FDB
            ExecutorService executor = Executors.newCachedThreadPool(
                    new ThreadFactory()
                    {
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(threadGroup, r);
                        }
                    }
            );

            // Explicit setup for passing executor
            test_FDB.startNetwork(executor);
            Cluster cluster = test_FDB.createCluster(null, executor);
            test_DB = cluster.openDatabase("DB".getBytes(Charset.forName("UTF8")));

            test_DIR = new ThreadLocal<FDBTestDirectory>();
        }
    }

    private static void test_CheckInitialized() {
        if(test_FDB == null) {
            String name = Util.class.getCanonicalName();
            System.err.println("ERROR: " + name + ": FDB not yet initialized");
            System.err.flush();
        }
    }

    /** Find the system thread group. By default, threads in this group won't trigger failures in Lucene test suite. */
    private static ThreadGroup findSystemThreadGroup() {
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        while(tg != null && !"system".equals(tg.getName())) {
            tg = tg.getParent();
        }
        return tg;
    }

    private static FDBTestDirectory test_GetDirectory() {
        if(test_DIR != null) {
            return test_DIR.get();
        }
        return null;
    }

    static Transaction test_CreateTransaction() {
        initFDB();
        return test_DB.createTransaction();
    }

    static void test_SetDirectory(FDBTestDirectory testDir) {
        test_CheckInitialized();
        test_DIR.set(testDir);
    }

    static void test_ClearDirectory(FDBTestDirectory testDir) {
        test_CheckInitialized();
        if(test_DIR.get() == testDir) {
            test_DIR.set(null);
        }
    }
}
