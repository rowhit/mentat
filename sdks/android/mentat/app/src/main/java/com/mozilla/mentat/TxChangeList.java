package com.mozilla.mentat;

import android.util.Log;

import com.sun.jna.Structure;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by emilytoop on 01/03/2018.
 */

public class TxChangeList extends Structure implements Closeable {
    public static class ByReference extends TxChangeList implements Structure.ByReference {
    }

    public static class ByValue extends TxChangeList implements Structure.ByValue {
    }

    public TxChange.ByReference reports;
    public int numberOfItems;
    // Used by the Swift counterpart, JNA does this for us automagically.
    public int len;

    public List<TxChange> getReports() {
        final TxChange[] array = (TxChange[]) reports.toArray(numberOfItems);
        return Arrays.asList(array);
    }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("reports", "numberOfItems", "len");
    }

    @Override
    public void close() {
        Log.i("TxChangeList", "close");
        final TxChange[] nativeReports = (TxChange[]) reports.toArray(numberOfItems);
        for (TxChange nativeReport : nativeReports) {
            nativeReport.close();
        }
    }
}
