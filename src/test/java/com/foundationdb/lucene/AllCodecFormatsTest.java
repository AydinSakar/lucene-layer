package com.foundationdb.lucene;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.util.NamedSPILoader;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class AllCodecFormatsTest extends SimpleTest
{
    @Parameters//(name="{0}")
    public static Collection<Object[]> params() {
        List<Object[]> params = new ArrayList<Object[]>();
        params.add(new Object[]{ FDBCodec.CONFIG_VALUE_ALL });
        params.add(new Object[]{ FDBCodec.CONFIG_VALUE_NONE });
        params.add(new Object[]{ FDBCodec.CONFIG_VALUE_DEFAULT });
        return params;
    }


    private final String codecConfig;

    public AllCodecFormatsTest(String codecConfig) {
        this.codecConfig = codecConfig;
    }


    /**
     * Codec's internal loader caches, and never reloads, the first instantiation of all defined codecs.
     * Subvert that so a simple unit test can hit all variations of FDBCodec behavior.
     */
    @Before
    public void cleanReloadCodecs() throws NoSuchFieldException, IllegalAccessException {
        // Make both fields accessible
        Field loaderField = Codec.class.getDeclaredField("loader");
        loaderField.setAccessible(true);
        Field servicesField = NamedSPILoader.class.getDeclaredField("services");
        servicesField.setAccessible(true);

        // Clear out the current map
        servicesField.set(loaderField.get(null), Collections.emptyMap());

        // Set the format config
        System.setProperty(FDBCodec.CONFIG_PROP_NAME, codecConfig);

        // Reload all (i.e. get new config in place)
        ((NamedSPILoader)loaderField.get(null)).reload(Thread.currentThread().getContextClassLoader());
    }
}
