package com.github.wanabe.presto_api_mapping;

import java.util.HashMap;

public class PrestoRecord extends HashMap<String, Object> {
    private static final long serialVersionUID = -4591086563468630500L;
    private final boolean isTerminator;

    PrestoRecord() {
        this(false);
    }

    PrestoRecord(final boolean isTerminator) {
        this.isTerminator = isTerminator;
    }

    boolean isTerminator() {
        return isTerminator;
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
