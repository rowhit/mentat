package com.mozilla.mentat;

import com.mozilla.mentat.rust.Store;
import com.mozilla.mentat.rust.Toodle;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Example local unit test, which will execute on the development machine (host).
 *
 * @see <a href="http://d.android.com/tools/testing">Testing documentation</a>
 */
public class ExampleUnitTest {
    @Test
    public void addition_isCorrect() throws Exception {
        assertEquals(4, 2 + 2);
    }


    @Test
    public void test_toodle() throws Exception {
        Store toodle = new Store("");
        assertNotNull(toodle);
    }
}