package com.mozilla.mentat;

import java.util.EventListener;

interface ExpectationEventListener extends EventListener {
    public void fulfill();
}

public class Expectation implements EventListener {
    public boolean isFulfilled = false;
    public void fulfill() {
        this.isFulfilled = true;
        synchronized (this) {
            notifyAll(  );
        }
    }
}
