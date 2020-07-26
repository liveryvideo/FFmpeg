/**
 * @author Stephan Hesse <stephan@emliri.com>
 * 
 * 
 * 
 * 
 * */

#pragma once

#include <stdio.h>
#include <string.h>

#include "exmg_queue.h"

#include "libavutil/time.h"
#include "libavformat/movenc.h"

//#include "MQTTClient.h"

// hard constants ("should be enough for everyone")
// - or switch to a dynamically allocated queuing/printing
#define EXMG_MESSAGE_BUFFER_SIZE 4096 // bytes, max size of one message
#define EXMG_MESSAGE_QUEUE_SIZE 0xFFFF // queueing capacity, maximum number of message items stored ahead publishing
// hard-constant atm, as in poor-mens thread signaling (see FIXME where used)
#define EXMG_MESSAGE_QUEUE_WORKER_POLL 0.020f // seconds -> default to 50fps to allow maximum needed accuracy

// defaults, for when not set by env
#define EXMG_MESSAGE_SEND_DELAY 10.0f // seconds // overrriden by FF_EXMG_KEY_MESSAGE_SEND_DELAY env var

#define EXMG_KEY_MESSAGE_FILENAME_DIR "./" // default value when env-var not set

// MQTT config (defaults)
#define EXMG_MQTT_URL "ssl://mqtt.liveryvideo.com:8883"
#define EXMG_MQTT_PASSWD "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyLWlkIjoidXNlcjIiLCJhdXRoLWdyb3VwcyI6InB1Ymxpc2g7c3Vic2NyaWJlIn0.BeBVt6WnpC9oNmd9-DLH7x4ROQaKUhQFOZYJBwyaV5GyYyXa3Hm-8GCppOFITp1-Djs5H5NrfThuDtDTaDr0vOZNPaSmkztW-fYWMZ7B4eUFEbnTwwoKzaZZRuQHqY_uFLyYM030VL3NMLhPdoyYnPrdHn-TnLHoZUkSh9gx0mBRJTA9fnurSgqRCM2ho4W5o_yQhB_ggIp04DgM0oZG8Qmts5nEmXLTTpEJs2wTla0aJ9a9bd2LBPF5jbXNkG8kI3BuH5-lq35EMH1UMMKBzqF4OiZ9pTc7GV9qhUDJvJOUlA3wxLWWLh4fuFQG-N90e_5Pj1xGNswIgAwzL7UehCBA03UFisY5AfNmoX0qQ_1Lbhl7xVnlMBtj4pSPCpQMHiuDkvQWvNFnmp9VCLcAQWvOXtasPl37Zd0Mwz1Nsn6ceUxBqUtCg26yA-v5Fs0nX5Z2UTCqtrLDjkNbtuMZTglkauVhZkvZp7ITC0bW_goNKSZgvRhGeh4cFkkUap059s7SUnrfWG7XMLoAsrG_nasfpNrHsHs2yZ7Hs8omYf7AkTP_vpXwNgBKfO5Y6wZyI50drTmZvVa1WXZp2YIbBSEz4rGa-lqAKVNKqMWK0g_3aUP6rQDbnenQRv7ZZ4x3W5QyYiUYD4PzkpWOSupIqgB968GTg_wrbPlGSysyR_U"
#define EXMG_MQTT_TOPIC "joep/test"
#define EXMG_MQTT_CLIENTID "user2"
#define EXMG_MQTT_USERNAME EXMG_MQTT_CLIENTID
#define EXMG_MQTT_VERSION MQTTVERSION_3_1_1

struct MOVTrack;
struct MOVMuxContext;

typedef struct ExmgMqttPubConfig {
    const char* url;
    const char* client_id;
    const char* user;
    const char* passwd; 
} ExmgMqttPubConfig;

typedef struct ExmgSecureSyncEncSession {
    float message_send_delay_secs;

    uint32_t fragments_per_key;

    uint32_t key_id_counter;
    uint32_t key_frag_counter;

    int64_t key_scope_first_pts;
    int64_t key_scope_duration;

    const uint8_t aes_key[AES_CTR_KEY_SIZE];
    const uint8_t aes_iv[AES_CTR_IV_SIZE];

    /* Contains ExmgSecureSyncScope pointers */
    ExmgQueue *scope_info_queue;

    pthread_mutex_t queue_lock;
    pthread_t queue_worker;
    pthread_cond_t queue_cond;

} ExmgSecureSyncEncSession;

typedef struct ExmgSecureSyncScope {
    uint8_t *media_key_message;
    int64_t media_time;
} ExmgSecureSyncScope;

static ExmgSecureSyncScope* exmg_secure_sync_scope_new(uint8_t *media_key_message, int64_t media_time)
{
    ExmgSecureSyncScope *sync_scope_info = (ExmgSecureSyncScope*) malloc(sizeof(ExmgSecureSyncScope));
    sync_scope_info->media_key_message = media_key_message;
    sync_scope_info->media_time = media_time;
    return sync_scope_info;
}

static void exmg_secure_sync_scope_dispose(ExmgSecureSyncScope* sync_scope_info)
{
    free(sync_scope_info);
}

#if 0
static int exmg_mqtt_client_connect(MQTTClient *client)
{
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    MQTTClient_SSLOptions ssl_opts = MQTTClient_SSLOptions_initializer;

    int rc = 0;
    const char* url = EXMG_MQTT_URL;

    conn_opts.keepAliveInterval = 1;
    conn_opts.username = EXMG_MQTT_USERNAME;
    conn_opts.password = EXMG_MQTT_PASSWD;
    conn_opts.MQTTVersion = EXMG_MQTT_VERSION;

    if (strncmp(url, "ssl://", 6) == 0
        || strncmp(url, "wss://", 6) == 0)
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

    conn_opts.cleansession = 1;
    rc = MQTTClient_connect(*client, &conn_opts);

    av_log(NULL, AV_LOG_VERBOSE, "exmg_mqtt_client_connect: %s\n", url);

    return rc;
}

static int exmg_mqtt_client_send(char* message)
{
    av_log(NULL, AV_LOG_VERBOSE, "exmg_mqtt_client_send: %s\n", message);

    MQTTClient client;
    MQTTProperties pub_props = MQTTProperties_initializer;
    MQTTClient_createOptions createOpts = MQTTClient_createOptions_initializer;
    MQTTClient_nameValue* infos = MQTTClient_getVersionInfo();

    int rc = 0;
    int result = 1;

    char* url;
    char* buffer = NULL;
    const char* version = NULL;

    char *topic = EXMG_MQTT_TOPIC;

    if (!message) {
        av_log(NULL, AV_LOG_ERROR, "exmg_mqtt_client_send: message argument is NULL\n");
        return 0;
    }

    url = EXMG_MQTT_URL;

    createOpts.MQTTVersion = EXMG_MQTT_VERSION;

    rc = MQTTClient_createWithOptions(&client, url,
        EXMG_MQTT_CLIENTID, MQTTCLIENT_PERSISTENCE_NONE,
            NULL, &createOpts);

    if (rc != MQTTCLIENT_SUCCESS)
    {
        av_log(NULL, AV_LOG_ERROR, "MQTTClient_createWithOptions result is failure\n");
        return 0;
    }

    if (exmg_mqtt_client_connect(&client) != MQTTCLIENT_SUCCESS) {

        av_log(NULL, AV_LOG_ERROR, "exmg_mqtt_client_connect result is failure\n");
        result = 0;

    } else {

        int data_len = 0;
        int delim_len = 0;

        if (message)
        {
            buffer = message;
            data_len = (int) strlen(message);
        }

        rc = MQTTClient_publish(client, topic, data_len, buffer, 0, 0, NULL);
        if (rc != 0)
        {
            rc = MQTTClient_publish(client, topic, data_len, buffer, 0, 0, NULL);
        }

    }

    rc = MQTTClient_disconnect(client, 0);
    if (rc != MQTTCLIENT_SUCCESS) {
        av_log(NULL, AV_LOG_ERROR, "MQTTClient_disconnect result is failure\n");
    }

    MQTTClient_destroy(&client);

    return result;
}
#endif

static void exmg_encrypt_buffer_aes_ctr(ExmgSecureSyncEncSession *session, uint8_t *buf, int size) {

    // read short-key data
    uint32_t media_encrypt_key;
    uint32_t media_encrypt_iv;
    memcpy(&media_encrypt_key, &session->aes_key, sizeof(media_encrypt_key));
    memcpy(&media_encrypt_iv, &session->aes_iv, sizeof(media_encrypt_iv));

    av_log(NULL, AV_LOG_VERBOSE, "Using key/iv pair to encrypt buffer (%d bytes): %u (0x%08X) / %u (0x%08X)\n",
        size,
        media_encrypt_key, media_encrypt_key,
        media_encrypt_iv, media_encrypt_iv);

    struct AVAESCTR *aes_ctx = av_aes_ctr_alloc();
    av_aes_ctr_init(aes_ctx, &session->aes_key[0]);
    av_aes_ctr_set_iv(aes_ctx, &session->aes_iv[0]);
    uint8_t *src_buf = (uint8_t*) malloc(size);
    memcpy(src_buf, buf, size);
    av_aes_ctr_crypt(aes_ctx, buf, src_buf, size);
    free(src_buf);
    av_aes_ctr_free(aes_ctx);
}

/**
 * POSIX FS convenience layer to write the file straight forward
 * given filename and message string.
 * Can also be used to abstract the writing of the file to an HTTP/WebDAV
 * or something of the likes, in case we want to convey/store the message
 * not directly on disk or send it somewhere instead.
 */
static int exmg_write_key_file(const char *filename, const uint8_t *data, int append)
{
    av_log(NULL, AV_LOG_INFO, "exmg_write_key_file: %s\n", filename);

    FILE *f;
    if (append) {
        f = fopen(filename, "a");
    } else {
        f = fopen(filename, "w");
    }

    if (f == NULL)
    {
        av_log(NULL, AV_LOG_ERROR, "exmg_write_key_file: failed to open file!\n");
        return 0;
    }

    fprintf(f, "%s\n", data);
    fclose(f);
    return 1;
}

/**
 * Will determine exact semantic filename based on message data buffer, and given track metadata.
 *
 * The idea is the key filename can be resolved back from an index of the metadata on either client side.
 *
 * This function will then directly call to `exmg_write_key_file` with its result.
 */
static void exmg_write_key_message(uint8_t* message_buffer, MOVTrack* track, int64_t message_media_time) {
    static const char* template = "exmg_key_%s_%d_%ld.json";

    const char *base_dir = getenv("FF_EXMG_KEY_FILE_OUT");
    if (base_dir == NULL) {
        base_dir = EXMG_KEY_MESSAGE_FILENAME_DIR;
    }

    const char* media_type = av_get_media_type_string(track->par->codec_type);

    char* index_path = (char*) malloc(strlen(base_dir) + 64);
    strcpy(index_path, base_dir);
    strcat(index_path, "exmg_key_index_");
    strcat(index_path, media_type); // we open one file per media-index to avoid concurrent access across the various movenc instances/thread-loops

    char *filename_template = (char*) malloc(strlen(base_dir) + strlen(template) + 1);
    strcpy(filename_template, base_dir);
    strcat(filename_template, template);

    int filename_len = strlen(filename_template) + 256; // we add 256 chars of leeway for the semantics we print in
    char *filename = (char*) malloc(filename_len + 1);
    int res = snprintf(filename, filename_len, filename_template,
        media_type,
        track->track_id,
        message_media_time
    );

    free(filename_template);

    if (res <= 0 || res >= filename_len){
        av_log(NULL, AV_LOG_ERROR, "Fatal error writing string, snprintf result value: %d\n", res);
        exit(1);
    }

    res = exmg_write_key_file(filename, message_buffer, 0);
    if (!res) {
        av_log(NULL, AV_LOG_ERROR, "Fatal error writing key-message!\n");
        exit(1);
    }

    res = exmg_write_key_file(index_path, (uint8_t*) filename, 1);
    if (!res) {
        av_log(NULL, AV_LOG_ERROR, "Fatal error writing key-message!\n");
        exit(1);
    }

    free(index_path);
    free(filename);
}

static void exmg_key_message_queue_pop(MOVMuxContext *mov)
{
    if (mov->nb_streams != 1) {
        av_log(mov, AV_LOG_WARNING, "Got %d streams, but should have exactly 1.", mov->nb_streams);
        exit(1);
        return;
    }

    ExmgSecureSyncEncSession *session = mov->exmg_key_sys;
    MOVTrack* track = &mov->tracks[0];
    if (track == NULL) {
        av_log(mov, AV_LOG_WARNING, "Going to publish media-key, but default track is NULL !\n");
        return;
    }

    float media_time_secs = (float) track->frag_start / (float) track->timescale;

    // Q: iterate over whole q until pop_index == push_index - 1 and while time_diff is > delay ?

    // pop message from queue with respect to delay set
    ff_mutex_lock(&session->queue_lock);
    // queue empty
    if (exmg_queue_is_empty(session->scope_info_queue)) {
        av_log(mov, AV_LOG_VERBOSE, "No key-messages to send!\n");
        ff_mutex_unlock(&session->queue_lock);
        return;
    }

    // peek into it first to compare time on queue with media-time
    ExmgSecureSyncScope *scope_info = (ExmgSecureSyncScope*) exmg_queue_peek(session->scope_info_queue);
    uint8_t* message_buffer = scope_info->media_key_message;
    int64_t message_media_time = scope_info->media_time;

    float next_popable_message_media_time = (float) message_media_time / (float) track->timescale;
    float time_diff = media_time_secs - next_popable_message_media_time;

    av_log(mov, AV_LOG_DEBUG, "(%s) Next pop'able message media-time: %.2f [s]\n",
        av_get_media_type_string(track->par->codec_type),
        next_popable_message_media_time);

    if (time_diff >= session->message_send_delay_secs) {

        // pop-off queue item we peeked before
        // (this is safe as we kept the mutex locked)
        exmg_queue_pop(session->scope_info_queue);
        ff_mutex_unlock(&session->queue_lock);
        // dipose of queue item
        exmg_secure_sync_scope_dispose(scope_info);

        av_log(mov, AV_LOG_INFO, "(%s) Publishing SecureSync key-message with:\nencryption-scope media-time=%.2f [s]\nat encoding-time=%.2f [s]\neffective key-publish-delay=%.2f [s]\n",
            av_get_media_type_string(track->par->codec_type),
            next_popable_message_media_time,
            media_time_secs,
            time_diff);

        if (!mov->exmg_key_system_mqtt_dry_run && getenv("FF_EXMG_KEYS_DRY_RUN") == NULL) {
            // we need the passed track parameters to create a unique indexable resource name
            exmg_write_key_message(message_buffer, track, message_media_time);

        } else {
            av_log(mov, AV_LOG_VERBOSE, "SecureSync dry-run, not sending.\n");
        }

        free(message_buffer); // free the buffer we malloc'd when put on the queue

    } else {

        av_log(mov, AV_LOG_VERBOSE, "(%s) SecureSync queue not pop'd, media-time difference is: %f secs\n",
            av_get_media_type_string(track->par->codec_type),
            time_diff);

        ff_mutex_unlock(&session->queue_lock);
    }
}

/**
 *  Gets called once per every fragment created from the movenc thread.
 *
 * */
static void exmg_key_message_queue_push(MOVMuxContext *mov, int tracks, int64_t mdat_size)
{
    if (mov->flags & FF_MOV_FLAG_DASH == 0) {
        return;
    }

    if (mov->nb_streams > 1) {
        av_log(mov, AV_LOG_ERROR, "SecureSync does not support multiple tracks per DASH fragment! Exiting process.");
        exit(1); //
        return;
    }

    if (mov->nb_streams != 1) {
        av_log(mov, AV_LOG_WARNING, "Got %d streams, but should have exactly 1.", mov->nb_streams);
        exit(1);
        return;
    }

    ExmgSecureSyncEncSession *session = mov->exmg_key_sys;
    MOVTrack* track = &mov->tracks[0];
    if (track == NULL) {
        av_log(mov, AV_LOG_WARNING, "Trying to push on queue, but default track is NULL !\n");
        return;
    }

    // generate new key when counter at zero
    if (session->key_frag_counter == 0) {

        session->key_scope_duration = 0;
        session->key_scope_first_pts = track->frag_start;
        session->key_id_counter++;

        //generate new key & IV: scale random int to ensured 32 bits
        uint32_t media_encrypt_key = (uint32_t) (rand() & 0xFFFF);
        uint32_t media_encrypt_iv = 0; // (uint32_t) rand();

        av_log(mov, AV_LOG_VERBOSE, "(%s) Set key/iv pair for %u next fragments: %u (0x%08X) / %u (0x%08X)\n",
            av_get_media_type_string(track->par->codec_type),
            session->fragments_per_key,
            media_encrypt_key, media_encrypt_key,
            media_encrypt_iv, media_encrypt_iv);

        // for now we zero pad and use only a "short" 4-byte key & IVs
        memset((void*) &session->aes_key, 0, sizeof(session->aes_key));
        memcpy((void*) &session->aes_key, &media_encrypt_key, sizeof(media_encrypt_key));
        memset((void*)&session->aes_iv, 0, sizeof(session->aes_iv));
        memcpy((void*) &session->aes_iv, &media_encrypt_iv, sizeof(media_encrypt_iv));
    }

    // incr frag counter
    session->key_frag_counter++;

    // update key-scope duration
    int64_t frag_duration = track->end_pts - track->frag_start;

    if (frag_duration == 0) { // Happens in LLS/streaming=1 mode for audio-type tracks
        // FIXME: This is a workaround, frag_duration should never be zero
        session->key_scope_duration = track->frag_start - session->key_scope_first_pts;
    } else {
        session->key_scope_duration += frag_duration;
    }

    av_log(mov,
        AV_LOG_VERBOSE,
        "(%s) Fragment duration: %lld, key-scope so-far duration: %lld (%u of %u fragments done in encryption-scope)\n",
        av_get_media_type_string(track->par->codec_type),
        frag_duration,
        session->key_scope_duration,
        session->key_frag_counter,
        session->fragments_per_key
    );

    // return if not at fragment count yet
    if (session->key_frag_counter < session->fragments_per_key) {
        return;
    } else {
        session->key_frag_counter = 0;
    }

    if (frag_duration == 0) {
        session->key_scope_duration++; // FIXME: this is really just a nifty little trick to fix lookup due to the bug noted above
                                       // in order to fix player lookup which will do: firstPts < keyBoundaryPts
    }

    // compute current media time
    float key_scope_start_secs = (float) session->key_scope_first_pts / (float) track->timescale;

    // read short-key data
    uint32_t key;
    uint32_t iv;
    memcpy(&key, &session->aes_key, sizeof(key));
    memcpy(&iv, &session->aes_iv, sizeof(iv));

    // alloc message buffer (free'd after having been pop'd from queue and sent)
    uint8_t *message_buffer = (uint8_t *) malloc(EXMG_MESSAGE_BUFFER_SIZE * sizeof(char));
    // write message data
    int printf_res = snprintf((char *) message_buffer, EXMG_MESSAGE_BUFFER_SIZE,
        "{\"creation_time\": %lld, \"fragment_info\": {\"track_id\": %d, \"media_time_secs\": %f, \
        \"first_pts\": %lld, \"duration\": %lld, \"timescale\": %u, \"codec_id\": %d, \"codec_type\": \"%s\", \"bitrate\": %lld}, \
        \"key_id\": %d, \"key\": \"0x%08X\", \"iv\": \"0x%08X\"}",
        av_gettime(),
        track->track_id,
        key_scope_start_secs,
        session->key_scope_first_pts,
        session->key_scope_duration,
        track->timescale,
        track->par->codec_id, // TODO: replace by codec_tag (4CC)
        av_get_media_type_string(track->par->codec_type),
        track->par->bit_rate,
        session->key_id_counter,
        key,
        iv
    );

    av_log(mov, AV_LOG_VERBOSE, "(%s) Wrote key-message: %s\n",
        av_get_media_type_string(track->par->codec_type),
        message_buffer);

    if (printf_res <= 0 || printf_res >= EXMG_MESSAGE_BUFFER_SIZE) {
        av_log(mov, AV_LOG_ERROR, "Fatal error writing string, snprintf result value: %d", printf_res);
        exit(1);
    }

    // push the message for this fragment on the queue
    ff_mutex_lock(&session->queue_lock);
    if (exmg_queue_is_full(session->scope_info_queue)) {
        av_log(mov, AV_LOG_ERROR, "SecureSync queue full. The delay set is probably too high. Exiting process now.");
        ff_mutex_unlock(&session->queue_lock);
        exit(1);
    }

    av_log(mov, AV_LOG_VERBOSE, "(%s) Pushing on queue\n",
        av_get_media_type_string(track->par->codec_type));

    ExmgSecureSyncScope *scope_info = exmg_secure_sync_scope_new(message_buffer, track->frag_start);
    exmg_queue_push(session->scope_info_queue, scope_info);
    ff_mutex_unlock(&session->queue_lock);

    av_log(mov, AV_LOG_VERBOSE,
        "(%s) Pushed key-message with scope starting at: %f [s] for track-id %d\n",
        av_get_media_type_string(track->par->codec_type),
        key_scope_start_secs,
        track->track_id);

}

static void exmg_key_message_queue_worker(MOVMuxContext* mov)
{
    // FIXME: instead of a sleep, we should use cond/wait thread signaling here

    unsigned int delay = EXMG_MESSAGE_QUEUE_WORKER_POLL * 1000000;
    while(1) {
        exmg_key_message_queue_pop(mov);
        // reschedule
        av_usleep(delay);
    }
}

static void exmg_secure_sync_enc_session_init(ExmgSecureSyncEncSession **session_ptr, MOVMuxContext *parent) {
    ExmgSecureSyncEncSession *session = *session_ptr = (ExmgSecureSyncEncSession *) malloc(sizeof(ExmgSecureSyncEncSession));
    memset(session, 0, sizeof(ExmgSecureSyncEncSession));

    char* message_send_delay = getenv("FF_EXMG_KEY_MESSAGE_SEND_DELAY");
    if (message_send_delay != NULL) {
        session->message_send_delay_secs = strtof(message_send_delay, NULL);
    } else {
        session->message_send_delay_secs = EXMG_MESSAGE_SEND_DELAY;
    }

    char* fragments_per_key = getenv("FF_EXMG_KEY_SCOPE_NB_OF_FRAGMENTS");
    if (fragments_per_key != NULL) {
        session->fragments_per_key = (uint32_t) atoi(fragments_per_key);
        if (session->fragments_per_key == 0) {
            session->fragments_per_key = 1;
        }
    } else {
        session->fragments_per_key = 1;
    }

    session->key_scope_duration = 0;
    session->key_frag_counter = 0;
    session->key_id_counter = 0;

    exmg_queue_init(&session->scope_info_queue, EXMG_MESSAGE_QUEUE_SIZE);

    ff_mutex_init(&session->queue_lock, NULL);
    pthread_create(&session->queue_worker, NULL, (void *(*)(void*)) exmg_key_message_queue_worker, (void*) parent);

    av_log(parent, AV_LOG_INFO,
        "Initialized SecureSync encode/encrypt context. Key-Publish-Delay=%f [s]; Fragments/Key=%d\n",
        session->message_send_delay_secs, session->fragments_per_key
    );

    srand(time(NULL));
}

// FIXME: also have deinit function