package com.github.wanabe.prestofetcher;

class PrestoQueueItem<T> {
    public final T val;
    PrestoQueueItem(T val) {
        this.val = val;
    }
}
