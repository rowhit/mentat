/* -*- Mode: Java; c-basic-offset: 4; tab-width: 20; indent-tabs-mode: nil; -*-
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.mentat;


import android.content.Context;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.util.Log;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import java.util.Date;
import java.util.UUID;

public class Mentat extends RustObject {

    static {
        System.loadLibrary("mentat_ffi");
    }

    public Mentat(String dbPath) {
        this.rawPointer = JNA.INSTANCE.store_open(dbPath);
    }

    public Mentat() {
        this.rawPointer = JNA.INSTANCE.store_open("");
    }

    public Mentat(Pointer rawPointer) { this.rawPointer = rawPointer; }

    public TxReport transact(String transaction) {
        RustResult result = JNA.INSTANCE.store_transact(this.rawPointer, transaction);
        if (result.isFailure()) {
            Log.i("Mentat", result.err);
            return null;
        }

        if (result.isSuccess()) {
            return new TxReport(result.ok);
        } else {
            return null;
        }
    }

    public long entIdForAttribute(String attribute) {
        return JNA.INSTANCE.store_entid_for_attribute(this.rawPointer, attribute);
    }

    public RustResult sync() {
        return JNA.INSTANCE.store_sync(rawPointer, "00000000-0000-0000-0000-000000000117", "http://mentat.dev.lcip.org/mentatsync/0.1");
    }

    public Query query(String query) {
        return new Query(JNA.INSTANCE.store_query(this.rawPointer, query));
    }

    public TypedValue valueForAttributeOfEntity(String attribute, long entid) {
        RustResult result = JNA.INSTANCE.store_value_for_attribute(this.rawPointer, entid, attribute);

        if (result.isSuccess()) {
            return new TypedValue(result.ok);
        }

        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }

        return null;
    }

    public void setDateForAttributeOfEntity(Date date, String attribute, long entid) {
        long timestamp = date.getTime() * 1000;
        RustResult result = JNA.INSTANCE.store_set_timestamp_for_attribute_on_entid(this.rawPointer, entid, attribute, timestamp);
        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }
    }

    public void setLongForAttributeOfEntity(long value, String attribute, long entid) {
        RustResult result = JNA.INSTANCE.store_set_long_for_attribute_on_entid(this.rawPointer, entid, attribute, value);
        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }
    }

    public void setReferenceForAttributeOfEntity(long value, String attribute, long entid) {
        RustResult result = JNA.INSTANCE.store_set_entid_for_attribute_on_entid(this.rawPointer, entid, attribute, value);
        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }
    }

    public void setKeywordReferenceForAttributeOfEntity(String value, String attribute, long entid) {
        RustResult result = JNA.INSTANCE.store_set_kw_ref_for_attribute_on_entid(this.rawPointer, entid, attribute, value);
        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }
    }

    public void setBooleanForAttributeOfEntity(boolean value, String attribute, long entid) {
        RustResult result = JNA.INSTANCE.store_set_boolean_for_attribute_on_entid(this.rawPointer, entid, attribute, value ? 1 : 0);
        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }
    }

    public void setDoubleForAttributeOfEntity(double value, String attribute, long entid) {
        RustResult result = JNA.INSTANCE.store_set_double_for_attribute_on_entid(this.rawPointer, entid, attribute, value);
        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }
    }

    public void setStringForAttributeOfEntity(String value, String attribute, long entid) {
        RustResult result = JNA.INSTANCE.store_set_string_for_attribute_on_entid(this.rawPointer, entid, attribute, value);
        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }
    }

    public void setUUIDForAttributeOfEntity(UUID value, String attribute, long entid) {
        RustResult result = JNA.INSTANCE.store_set_uuid_for_attribute_on_entid(this.rawPointer, entid, attribute, value.toString());
        if (result.isFailure()) {
            Log.i("Mentat", result.err);
        }
    }

    public void registerObserver(String key, String[] attributes, TxObserverCallback callback) {
        // turn string array into int array
        long[] attrEntids = new long[attributes.length];
        for(int i = 0; i < attributes.length; i++) {
            attrEntids[i] = JNA.INSTANCE.store_entid_for_attribute(this.rawPointer, attributes[i]);
        }
        Log.i("Mentat", "Registering observer {" + key + "} for attributes:");
        for (int i = 0; i < attrEntids.length; i++) {
            Log.i("Mentat", "entid: " + attrEntids[i]);
        }
        final Pointer entidsNativeArray = new Memory(8 * attrEntids.length);
        entidsNativeArray.write(0, attrEntids, 0, attrEntids.length);
        JNA.INSTANCE.store_register_observer(rawPointer, key, entidsNativeArray, attrEntids.length, callback);
    }

    public void unregisterObserver(String key) {
        JNA.INSTANCE.store_unregister_observer(rawPointer, key);
    }

    @Override
    public void close() {
        Log.i("Mentat", "close");
        if (this.rawPointer != null) {
            JNA.INSTANCE.store_destroy(this.rawPointer);
        }
    }
}
