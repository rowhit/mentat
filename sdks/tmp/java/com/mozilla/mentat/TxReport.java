package com.mozilla.mentat;

import android.util.Log;

import com.sun.jna.Pointer;

import java.util.Date;

public class TxReport extends RustObject {

    private Long txId;
    private Date txInstant;


    public TxReport(Pointer pointer) {
        this.rawPointer = pointer;
    }

    public Long getTxId() {
        if (this.txId == null) {
            this.txId = JNA.INSTANCE.tx_report_get_entid(this.rawPointer);
        }

        return this.txId;
    }

    public Date getTxInstant() {
        if (this.txInstant == null) {
            this.txInstant = new Date(JNA.INSTANCE.tx_report_get_tx_instant(this.rawPointer));
        }
        return this.txInstant;
    }

    public Long getEntidForTempId(String tempId) {
        return JNA.INSTANCE.tx_report_entity_for_temp_id(this.rawPointer, tempId);
    }

    @Override
    public void close() {
        Log.i("TxReport", "close");
        if (this.rawPointer != null) {
            JNA.INSTANCE.tx_report_destroy(this.rawPointer);
        }
    }
}
