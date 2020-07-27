/**
 * @author Stephan Hesse <stephan@emliri.com>
 *
 * */

#pragma once

#include <string.h>

#include "libavutil/log.h"

#include "MQTTClient.h"

#define MQTT_VERSION MQTTVERSION_3_1_1
#define MQTT_CLIENT_DISCONNECT_TIMEOUT_MS 2000

// MQTT config (defaults)
#define EXMG_MQTT_URL "ssl://mqtt.liveryvideo.com:8883"
#define EXMG_MQTT_CLIENTID "user2"
#define EXMG_MQTT_USERNAME EXMG_MQTT_CLIENTID
#define EXMG_MQTT_PASSWD "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyLWlkIjoidXNlcjIiLCJhdXRoLWdyb3VwcyI6InB1Ymxpc2g7c3Vic2NyaWJlIn0.BeBVt6WnpC9oNmd9-DLH7x4ROQaKUhQFOZYJBwyaV5GyYyXa3Hm-8GCppOFITp1-Djs5H5NrfThuDtDTaDr0vOZNPaSmkztW-fYWMZ7B4eUFEbnTwwoKzaZZRuQHqY_uFLyYM030VL3NMLhPdoyYnPrdHn-TnLHoZUkSh9gx0mBRJTA9fnurSgqRCM2ho4W5o_yQhB_ggIp04DgM0oZG8Qmts5nEmXLTTpEJs2wTla0aJ9a9bd2LBPF5jbXNkG8kI3BuH5-lq35EMH1UMMKBzqF4OiZ9pTc7GV9qhUDJvJOUlA3wxLWWLh4fuFQG-N90e_5Pj1xGNswIgAwzL7UehCBA03UFisY5AfNmoX0qQ_1Lbhl7xVnlMBtj4pSPCpQMHiuDkvQWvNFnmp9VCLcAQWvOXtasPl37Zd0Mwz1Nsn6ceUxBqUtCg26yA-v5Fs0nX5Z2UTCqtrLDjkNbtuMZTglkauVhZkvZp7ITC0bW_goNKSZgvRhGeh4cFkkUap059s7SUnrfWG7XMLoAsrG_nasfpNrHsHs2yZ7Hs8omYf7AkTP_vpXwNgBKfO5Y6wZyI50drTmZvVa1WXZp2YIbBSEz4rGa-lqAKVNKqMWK0g_3aUP6rQDbnenQRv7ZZ4x3W5QyYiUYD4PzkpWOSupIqgB968GTg_wrbPlGSysyR_U"
#define EXMG_MQTT_TOPIC "joep/test"

typedef struct ExmgMqttPubConfig {
    const char* client_id;
    const char* user;
    const char* passwd;
    const char* topic;
} ExmgMqttPubConfig;

typedef struct ExmgMqttServiceInfo {
    const char* url;
    ExmgMqttPubConfig pubConf;
} ExmgMqttServiceInfo;

#define EXMG_MQTT_PUB_CONFIG_DEFAULT_INIT {EXMG_MQTT_CLIENTID, EXMG_MQTT_USERNAME, EXMG_MQTT_PASSWD, EXMG_MQTT_TOPIC}
#define EXMG_MQTT_SERVICE_INFO_DEFAULT_INIT {EXMG_MQTT_URL, EXMG_MQTT_PUB_CONFIG_DEFAULT_INIT}

typedef struct ExmgMqttPubContext {
    const char *server_uri;
    ExmgMqttPubConfig config;
    int is_tls;
    int is_connected;
    MQTTClient client;
} ExmgMqttPubContext;

typedef struct ExmgMqttPubContextCollection {
    ExmgMqttPubContext** ctxs;
    size_t *ctxs_next_idx;
    size_t ctxs_size;
} ExmgMqttPubContextCollection;

static ExmgMqttPubContextCollection exmg_mqtt_get_pub_contexts() {
    static ExmgMqttPubContext* ctx_ptrs[0xFF];
    static size_t ctx_ptrs_idx = 0;
    ExmgMqttPubContextCollection c = {ctx_ptrs, &ctx_ptrs_idx, sizeof(ctx_ptrs)};
    return c;
}

static void exmg_mqtt_pub_context_init(ExmgMqttPubContext **ptr, const char* url, ExmgMqttPubConfig config) {

    ExmgMqttPubContextCollection c = exmg_mqtt_get_pub_contexts();
    for (size_t i = 0; i < *c.ctxs_next_idx; i++) {
        ExmgMqttPubContext *existing_ctx = c.ctxs[i];
        if (strcmp(existing_ctx->server_uri, url) == 0
            && (existing_ctx->config.topic == config.topic)
            && (existing_ctx->config.client_id == config.client_id)) {
            av_log(NULL, AV_LOG_INFO,
                "Reusing existing MQTT pub context @ %p for server URL (matching config): %s\n", existing_ctx, url);
            *ptr = existing_ctx;
            return;
        }
    }

    ExmgMqttPubContext *ctx = (ExmgMqttPubContext*) malloc(sizeof(ExmgMqttPubContext));

    ctx->server_uri = url;
    ctx->config = config;
    ctx->is_tls = strncmp(url, "ssl://", 6) == 0 || strncmp(url, "wss://", 6) == 0;
    ctx->is_connected = 0;

    MQTTClient_createOptions opts = MQTTClient_createOptions_initializer;
    opts.MQTTVersion = MQTT_VERSION;

    int rc = MQTTClient_createWithOptions(&ctx->client,
        ctx->server_uri,
            config.client_id,
                MQTTCLIENT_PERSISTENCE_NONE, NULL, // Q: what does this do/not do ?
                    &opts);

    if (rc != MQTTCLIENT_SUCCESS)
    {
        av_log(NULL, AV_LOG_ERROR, "MQTTClient_createWithOptions failed with context @ %p\n", ctx);
        free(ctx);
        *ptr = NULL;
        return;
    }

    av_log(NULL, AV_LOG_INFO, "MQTTClient_createWithOptions(%p) success with context @ %p\n", ctx->client, ctx);

    c.ctxs[(*c.ctxs_next_idx)++] = *ptr = ctx;
}

static void exmg_mqtt_pub_context_deinit(ExmgMqttPubContext **ptr) {
    ExmgMqttPubContext* ctx = *ptr;
    if (ctx->is_connected && MQTTClient_isConnected(ctx->client)) {
        int rc = MQTTClient_disconnect(ctx->client, MQTT_CLIENT_DISCONNECT_TIMEOUT_MS);
        if (rc != MQTTCLIENT_SUCCESS) {
            av_log(NULL, AV_LOG_ERROR, "MQTTClient_disconnect failed\n");
            return;
        }
        ctx->is_connected = 0;
    }
    MQTTClient_destroy(&ctx->client);
    free(ctx);
    *ptr = NULL;
}

static int exmg_mqtt_pub_connect(ExmgMqttPubContext *ctx)
{
    if (ctx->is_connected) {
        av_log(NULL, AV_LOG_WARNING, "exmg_mqtt_pub_connect(%p) called but already connected to: %s\n", ctx, ctx->server_uri);
        return ctx->is_connected;
    }

    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    conn_opts.MQTTVersion = MQTT_VERSION;
    conn_opts.keepAliveInterval = 1;
    conn_opts.username = EXMG_MQTT_USERNAME;
    conn_opts.password = EXMG_MQTT_PASSWD;
    conn_opts.cleansession = 1;

    MQTTClient_SSLOptions ssl_opts = MQTTClient_SSLOptions_initializer;
    if (ctx->is_tls)
    {
        ssl_opts.verify = 1;
        ssl_opts.CApath = NULL;
        ssl_opts.keyStore = NULL;
        ssl_opts.trustStore = NULL;
        ssl_opts.privateKey = NULL;
        ssl_opts.privateKeyPassword = NULL;
        ssl_opts.enabledCipherSuites = NULL;
        conn_opts.ssl = &ssl_opts;
    }

    av_log(NULL, AV_LOG_INFO, "Calling MQTTClient_connect with handle @ %p ...\n", ctx->client);

    int rc = MQTTClient_connect(ctx->client, &conn_opts);
    if (rc == MQTTCLIENT_SUCCESS) {
        av_log(NULL, AV_LOG_INFO, "MQTTClient_connect success to: %s\n", ctx->server_uri);
        ctx->is_connected = 1;
    } else {
        av_log(NULL, AV_LOG_ERROR, "MQTTClient_connect failed (error-code=%d) to: %s\n ", rc, ctx->server_uri);
        ctx->is_connected = 0;
    }
    return ctx->is_connected;
}

static int exmg_mqtt_pub_send(ExmgMqttPubContext *ctx, uint8_t* message_data, int message_length, int retry_counter)
{
    if (retry_counter == 0) {
        av_log(NULL, AV_LOG_ERROR, "exmg_mqtt_pub_send(%p, %p): abandoning retrials, permanently failed", ctx, message_data);
        return 0;
    } else if (retry_counter < 0) {
        retry_counter = 3;
    }

    if (!ctx->is_connected && exmg_mqtt_pub_connect(ctx) != MQTTCLIENT_SUCCESS) {
        av_log(NULL, AV_LOG_ERROR, "exmg_mqtt_pub_connect(%p) failed, retry-counter = %d \n", ctx, retry_counter);
        return exmg_mqtt_pub_send(ctx, message_data, message_length, --retry_counter);
    }

    int rc = MQTTClient_publish(ctx->client, ctx->config.topic, message_length, message_data, 0, 0, NULL);
    if (rc != MQTTCLIENT_SUCCESS)
    {
        av_log(NULL, AV_LOG_ERROR,
            "MQTTClient_publish on context @ %p failed (error-code = %d), retry-counter = %d\n", ctx, rc, retry_counter);
        ctx->is_connected = 0; // try to reconnect
        return exmg_mqtt_pub_send(ctx, message_data, message_length, --retry_counter);
    }

    av_log(NULL, AV_LOG_DEBUG,
        "exmg_mqtt_client_send(%p): published message data @ %p (length = %d bytes)\n", ctx, message_data, message_length);

    return 1;
}
