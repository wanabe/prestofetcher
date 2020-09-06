package com.github.wanabe.prestofetcher;

import java.io.IOException;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.Request.Builder;

class PrestoSession {
    private final OkHttpClient client;
    private final String host;
    private final String user;
    private final String catalog;
    private final String schema;

    public PrestoSession(final String user, final String catalog, final String schema, final String host) {
        client = new OkHttpClient();
        this.user = user;
        this.catalog = catalog;
        this.schema = schema;
        this.host = host;
    }

    public PrestoSession(final String user, final String catalog, final String schema) {
        this(user, catalog, schema, null);
    }

    public String query(final String query) throws IOException {
        final RequestBody body = RequestBody.create(query, MediaType.get("text/plain; charset=utf-8"));
        final Builder requestBuilder = new Request.Builder().url(host + "/v1/statement").post(body);
        return fetchResponse(requestBuilder);
    }

    public String get(final String url) throws IOException {
        final Builder requestBuilder = new Request.Builder().url(url).get();
        return fetchResponse(requestBuilder);
    }

    private String fetchResponse(final Builder requestBuilder) throws IOException {
        final Request request = requestBuilder.addHeader("X-Presto-User", user).addHeader("X-Presto-Catalog", catalog)
                .addHeader("X-Presto-Schema", schema).build();
        final Response response = client.newCall(request).execute();
        return response.body().string();
    }
}
