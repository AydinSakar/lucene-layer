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
