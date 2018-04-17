/* -*- Mode: Java; c-basic-offset: 4; tab-width: 20; indent-tabs-mode: nil; -*-
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.mentat;

import android.util.Log;
import com.sun.jna.Pointer;

import java.util.Date;
import java.util.UUID;

public class TupleResult extends RustObject {

    public TupleResult(Pointer pointer) {
        this.rawPointer = pointer;
    }

    public TypedValue get(Integer index) {
        this.validate();
        Pointer pointer = JNA.INSTANCE.value_at_index(this.rawPointer, index);
        if (pointer == null) {
            return null;
        }
        return new TypedValue(pointer);
    }

    public Long asLong(Integer index) {
        this.validate();
        return JNA.INSTANCE.value_at_index_as_long(this.rawPointer, index);
    }

    public Long asEntid(Integer index) {
        this.validate();
        return JNA.INSTANCE.value_at_index_as_entid(this.rawPointer, index);
    }

    public String asKeyword(Integer index) {
        this.validate();
        return JNA.INSTANCE.value_at_index_as_kw(this.rawPointer, index);
    }

    public Boolean asBool(Integer index) {
        this.validate();
        return JNA.INSTANCE.value_at_index_as_boolean(this.rawPointer, index) == 0 ? false : true;
    }

    public Double asDouble(Integer index) {
        this.validate();
        return JNA.INSTANCE.value_at_index_as_double(this.rawPointer, index);
    }

    public Date asDate(Integer index) {
        this.validate();
        return new Date(JNA.INSTANCE.value_at_index_as_timestamp(this.rawPointer, index));
    }

    public String asString(Integer index) {
        this.validate();
        return JNA.INSTANCE.value_at_index_as_string(this.rawPointer, index);
    }

    public UUID asUUID(Integer index) {
        this.validate();
        return UUID.fromString(JNA.INSTANCE.value_at_index_as_uuid(this.rawPointer, index));
    }

    @Override
    public void close() {
        Log.i("TupleResult", "close");
        if (this.rawPointer != null) {
            JNA.INSTANCE.typed_value_list_destroy(this.rawPointer);
        }
    }
}
