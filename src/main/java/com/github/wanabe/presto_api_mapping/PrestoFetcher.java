package com.github.wanabe.presto_api_mapping;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.Request.Builder;

import com.github.wanabe.presto_api_mapping.PrestoStatement.Column;

class PrestoFetcher extends Thread {
    private final LinkedBlockingQueue<PrestoRecord> queue;
    private final long timeout;
    private final TimeUnit unit;
    private final OkHttpClient client;
    private final PrestoStatement statement;

    public PrestoFetcher() {
        this(null, 0L, null);
    }

    public PrestoFetcher(final String uri, final long timeout, final TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
        this.queue = new LinkedBlockingQueue<>();
        client = new OkHttpClient();
        if (uri == null) {
            statement = new PrestoStatement();
        } else {
            statement = new PrestoStatement(uri);
        }
    }

    public boolean isClosed() {
        return statement.isClosed();
    }

    public PrestoRecord poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        PrestoRecord record = null;
        if (isClosed() && queue.isEmpty()) {
            return null;
        }
        if (timeout == 0) {
            record = queue.poll();
        } else {
            record = queue.poll(timeout, unit);
        }
        if (record != null && record.isTerminator()) {
            return null;
        }
        return record;
    }

    public PrestoRecord poll() throws InterruptedException {
        return poll(timeout, unit);
    }

    void close() {
        queue.add(new PrestoRecord(true));
        statement.close();
    }

    @Override
    public void run() {
        while (!isClosed()) {
            fetchStatement(new Request.Builder().url(statement.getNextUri()).get());
        }
    }

    public String query(final String host, final String query) {
        final RequestBody body = RequestBody.create(query, MediaType.get("text/plain; charset=utf-8"));
        final Builder requestBuilder = new Request.Builder().url(host + "/v1/statement").post(body);
        fetchStatement(requestBuilder);
        return statement.getNextUri();
    }

    private void fetchStatement(final Builder requestBuilder) {
        final Request request = requestBuilder.addHeader("X-Presto-User", "presto")
                .addHeader("X-Presto-Catalog", "mysql").addHeader("X-Presto-Schema", "test").build();
        try {
            final ObjectMapper mapper = new ObjectMapper();
            final Response response = client.newCall(request).execute();
            final String responseBody = response.body().string();
            statement.clear();
            mapper.readerForUpdating(statement).readValue(responseBody);
        } catch (final IOException e) {
            close();
            return;
        }
        processStatement();
    }

    private void processStatement() {
        if (statement.getData() != null) {
            for (final List<Object> values : statement.getData()) {
                final PrestoRecord record = new PrestoRecord();
                final Iterator<Object> valueIterator = values.iterator();
                for (final Column column : statement.getColumns()) {
                    record.put(column.getName(), valueIterator.next());
                }
                queue.add(record);
            }
        }
        if (statement.getNextUri() == null) {
            close();
        }
    }
}
