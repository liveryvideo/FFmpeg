#pragma once

#include "libavutil/time.h"

#include "movenc.h"

#include "MQTTClient.h"

// hard constants ("should be enough for everyone")
// - or switch to a dynamically allocated queuing/printing
#define EXMG_MESSAGE_BUFFER_SIZE 4096
#define EXMG_MESSAGE_QUEUE_SIZE 0xFFFF

// defaults, for when not set by env
#define EXMG_MESSAGE_SEND_DELAY 10.0f // seconds
#define EXMG_KEY_QUEUE_WORKER_POLL 1.0f // seconds

// MQTT config
///*
#define EXMG_MQTT_URL "ssl://mqtt.liveryvideo.com:8883"
#define EXMG_MQTT_PASSWD "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyLWlkIjoidXNlcjIiLCJhdXRoLWdyb3VwcyI6InB1Ymxpc2g7c3Vic2NyaWJlIn0.BeBVt6WnpC9oNmd9-DLH7x4ROQaKUhQFOZYJBwyaV5GyYyXa3Hm-8GCppOFITp1-Djs5H5NrfThuDtDTaDr0vOZNPaSmkztW-fYWMZ7B4eUFEbnTwwoKzaZZRuQHqY_uFLyYM030VL3NMLhPdoyYnPrdHn-TnLHoZUkSh9gx0mBRJTA9fnurSgqRCM2ho4W5o_yQhB_ggIp04DgM0oZG8Qmts5nEmXLTTpEJs2wTla0aJ9a9bd2LBPF5jbXNkG8kI3BuH5-lq35EMH1UMMKBzqF4OiZ9pTc7GV9qhUDJvJOUlA3wxLWWLh4fuFQG-N90e_5Pj1xGNswIgAwzL7UehCBA03UFisY5AfNmoX0qQ_1Lbhl7xVnlMBtj4pSPCpQMHiuDkvQWvNFnmp9VCLcAQWvOXtasPl37Zd0Mwz1Nsn6ceUxBqUtCg26yA-v5Fs0nX5Z2UTCqtrLDjkNbtuMZTglkauVhZkvZp7ITC0bW_goNKSZgvRhGeh4cFkkUap059s7SUnrfWG7XMLoAsrG_nasfpNrHsHs2yZ7Hs8omYf7AkTP_vpXwNgBKfO5Y6wZyI50drTmZvVa1WXZp2YIbBSEz4rGa-lqAKVNKqMWK0g_3aUP6rQDbnenQRv7ZZ4x3W5QyYiUYD4PzkpWOSupIqgB968GTg_wrbPlGSysyR_U"
#define EXMG_MQTT_TOPIC "joep/test"
#define EXMG_MQTT_CLIENTID "user2"
#define EXMG_MQTT_USERNAME EXMG_MQTT_CLIENTID
#define EXMG_MQTT_VERSION MQTTVERSION_3_1_1
//*/

/*
#define EXMG_MQTT_URL "wss://10.211.55.6:8081"
#define EXMG_MQTT_TOPIC "test"
#define EXMG_MQTT_CLIENTID ""
#define EXMG_MQTT_USERNAME NULL
#define EXMG_MQTT_PASSWD NULL
#define EXMG_MQTT_VERSION MQTTVERSION_3_1_1
//*/

#define EXMG_KEY_MESSAGE_FILENAME_DIR "./" // default value when env-var not set

struct MOVTrack;
struct MOVMuxContext;

typedef struct ExmgKeySystemEncryptSession {
    float message_send_delay_secs;

    uint32_t exmg_key_id_counter;

    uint8_t exmg_aes_key[AES_CTR_KEY_SIZE];
    uint8_t exmg_aes_iv[AES_CTR_IV_SIZE];
    // TODO: use one array of data struct instead of many arrays here
    char *exmg_messages_queue[EXMG_MESSAGE_QUEUE_SIZE]; // can't be more than MAX_INT32
    int64_t exmg_messages_queue_media_time[EXMG_MESSAGE_QUEUE_SIZE];
    int32_t exmg_messages_queue_media_key[EXMG_MESSAGE_QUEUE_SIZE];
    int32_t exmg_messages_queue_push_idx;
    int32_t exmg_messages_queue_pop_idx;

    pthread_mutex_t exmg_queue_lock;
    pthread_t exmg_queue_worker;
    pthread_cond_t exmg_queue_cond;
} ExmgKeySystemEncryptSession;

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

/**
 * POSIX FS convenience layer to write the file straight forward
 * given filename and message string.
 * Can also be used to abstract the writing of the file to an HTTP/WebDAV
 * or something of the likes, in case we want to convey/store the message
 * not directly on disk or send it somewhere instead.
 */
static int exmg_write_key_file(const char *filename, char *data, int append)
{
    av_log(NULL, AV_LOG_VERBOSE, "exmg_write_key_file: %s\n", filename);

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
static void exmg_write_key_message(const char* message_buffer, MOVTrack* track, int64_t message_media_time) {
    static const char* template = "exmg_key_%s_%d_%ld.json";

    char *base_dir = getenv("FF_EXMG_KEY_FILE_OUT");
    if (base_dir == NULL) {
        base_dir = EXMG_KEY_MESSAGE_FILENAME_DIR;
    }

    const char* media_type = av_get_media_type_string(track->par->codec_type);

    char* index_path = malloc(strlen(base_dir) + 64);
    strcpy(index_path, base_dir);
    strcat(index_path, "exmg_key_index_");
    strcat(index_path, media_type); // we open one file per media-index to avoid concurrent access across the various movenc instances/thread-loops

    char *filename_template = malloc(strlen(base_dir) + strlen(template) + 1);
    strcpy(filename_template, base_dir);
    strcat(filename_template, template);

    int filename_len = strlen(filename_template) + 256; // we add 256 chars of leeway for the semantics we print in
    char *filename = malloc(filename_len + 1);
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

    res = exmg_write_key_file(index_path, filename, 1);
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
        av_log(mov, AV_LOG_WARNING, "Track has %d tracks, but should have exactly 1.", mov->nb_streams);
        return;
    }

    ExmgKeySystemEncryptSession *session = mov->exmg_key_sys;

    MOVTrack* track = &mov->tracks[0];

    float media_time_secs = (float) track->frag_start / (float) track->timescale;

    // Q: iterate over whole q until pop_index == push_index - 1 and while time_diff is > delay ?

    // pop message from queue with respect to delay set
    ff_mutex_lock(&session->exmg_queue_lock);
    int32_t message_q_pop_index = session->exmg_messages_queue_pop_idx + 1;
    char* message_buffer = session->exmg_messages_queue[message_q_pop_index];
    int64_t message_media_time = session->exmg_messages_queue_media_time[message_q_pop_index];

    if (message_buffer == NULL) {
        //av_log(mov, AV_LOG_VERBOSE, "No key-messages to send!\n");
        ff_mutex_unlock(&session->exmg_queue_lock);
        return;
    }

    float next_popable_message_media_time = (float) message_media_time / (float) track->timescale;
    float time_diff = media_time_secs - next_popable_message_media_time;

    av_log(mov, AV_LOG_VERBOSE, "Next pop'able message media time: %f\n", next_popable_message_media_time);

    if (time_diff >= session->message_send_delay_secs) {

        av_log(mov, AV_LOG_VERBOSE, "EXMG key-message queue pop, media-time difference is: %f secs\n", time_diff);

        session->exmg_messages_queue_pop_idx++;
        ff_mutex_unlock(&session->exmg_queue_lock);

        if (!mov->exmg_key_system_mqtt_dry_run && getenv("FF_EXMG_KEYS_DRY_RUN") == NULL) {

            //exmg_mqtt_client_send(message_buffer);

            // we need the track info to create a unique indexable filename
            exmg_write_key_message(message_buffer, track, message_media_time);

        } else {
            av_log(mov, AV_LOG_VERBOSE, "EXMG key-system dry-run, not sending.");
        }

        free(message_buffer); // free the buffer we malloc'd when put on the queue

    } else {
        ff_mutex_unlock(&session->exmg_queue_lock);
        av_log(mov, AV_LOG_VERBOSE, "EXMG key-message queue not pop'd, media-time difference is: %f secs\n", time_diff);
    }
}

static void exmg_store_key_and_iv(MOVMuxContext *mov) {
    // TODO: generate proper 16-byte length key
}

static void exmg_key_message_queue_push(MOVMuxContext *mov, int tracks, int64_t mdat_size)
{
    if (mov->flags & FF_MOV_FLAG_DASH == 0) {
        return;
    }

    if (mov->nb_streams > 1) {
        av_log(mov, AV_LOG_ERROR, "EXMG key system does not support multiple tracks per DASH fragment! Exiting process.");
        exit(0); //
        return;
    }

    if (mov->nb_streams != 1) {
        av_log(mov, AV_LOG_WARNING, "Track has %d tracks, but should have exactly 1.", mov->nb_streams);
        return;
    }

    av_log(mov, AV_LOG_DEBUG, "Creating key-message for fragment");

    ExmgKeySystemEncryptSession *session = mov->exmg_key_sys;

    MOVTrack* track = &mov->tracks[0];

    // compute current media time
    float media_time_secs = (float) track->frag_start / (float) track->timescale;

    // alloc message buffer (free'd after having been pop'd from queue and sent)
    char *message_buffer = (char *) malloc(EXMG_MESSAGE_BUFFER_SIZE * sizeof(char));

    //generate key & IV: scale random int to ensured 32 bits
    uint32_t media_encrypt_key = (uint32_t) roundf((float) UINT32_MAX * ((float) rand() / (float) RAND_MAX));
    uint32_t media_encrypt_iv = (uint32_t) roundf((float) UINT32_MAX * ((float) rand() / (float) RAND_MAX));

    int64_t frag_duration = track->end_pts - track->frag_start;

    // write message data
    int printf_res = snprintf(message_buffer, EXMG_MESSAGE_BUFFER_SIZE,
        "{\"creation_time\": %ld, \"fragment_info\": {\"track_id\": %d, \"media_time_secs\": %f, \
        \"first_pts\": %ld, \"duration\": %ld, \"timescale\": %u, \"codec_id\": %d, \"codec_type\": \"%s\", \"bitrate\": %ld}, \
        \"key_id\": %d, \"key\": %u, \"iv\": %u}",
        av_gettime(),
        track->track_id,
        media_time_secs,
        track->frag_start,
        frag_duration,
        track->timescale,
        track->par->codec_id, // TODO: replace by codec_tag (4CC)
        av_get_media_type_string(track->par->codec_type),
        track->par->bit_rate,
        session->exmg_key_id_counter,
        media_encrypt_key,
        media_encrypt_iv
    );
    
    if (printf_res <= 0 || printf_res >= EXMG_MESSAGE_BUFFER_SIZE) {
        av_log(mov, AV_LOG_ERROR, "Fatal error writing string, snprintf result value: %d", printf_res);
        exit(1);
    }

    // update key-id counter and key/iv storage
    session->exmg_key_id_counter++;

    // for now we zero pad and use only a "short" 4-byte key & IVs
    memset(session->exmg_aes_key, 0, sizeof(session->exmg_aes_key));
    memcpy(session->exmg_aes_key, &media_encrypt_key, sizeof(media_encrypt_key));
    memset(session->exmg_aes_iv, 0, sizeof(session->exmg_aes_iv));
    memcpy(session->exmg_aes_iv, &media_encrypt_iv, sizeof(media_encrypt_iv));

    // push the message for this fragment on the queue
    ff_mutex_lock(&session->exmg_queue_lock);

    session->exmg_messages_queue_media_time[session->exmg_messages_queue_push_idx] = track->frag_start;
    session->exmg_messages_queue_media_key[session->exmg_messages_queue_push_idx] = media_encrypt_key;
    session->exmg_messages_queue[session->exmg_messages_queue_push_idx] = message_buffer;
    session->exmg_messages_queue_push_idx++;

    // handle push index overflow
    if (session->exmg_messages_queue_push_idx >= EXMG_MESSAGE_QUEUE_SIZE) {
        // catch edge condition: queue overflows without anything read yet
        if (session->exmg_messages_queue_pop_idx == -1) {
            av_log(mov, AV_LOG_ERROR, "EXMG key-message queue overflowed. The delay set is probably too high. Exiting process now.");
            ff_mutex_unlock(&session->exmg_queue_lock);
            exit(0);
        }
        // normal operation, allow write over queue in circular way
        session->exmg_messages_queue_push_idx = 0;
    }
    // general case: queue is full when push index equals pop index
    if (session->exmg_messages_queue_push_idx == session->exmg_messages_queue_pop_idx) {
        av_log(mov, AV_LOG_ERROR, "EXMG key-message queue full. The delay set is probably too high. Exiting process now.");
        ff_mutex_unlock(&session->exmg_queue_lock);
        exit(0);
    }

    ff_mutex_unlock(&session->exmg_queue_lock);

    av_log(mov, AV_LOG_VERBOSE,
        "Pushed message on queue with timestamp: %f for track-id %d of type: %s\n",
        media_time_secs,
        track->track_id,
        av_get_media_type_string(track->par->codec_type));

    #if 0
        exmg_mqtt_queue_pop(mov);
    #endif

    #if 0
        mov_write_exmg_tag(pb, mov);
    #endif

}


static void exmg_encrypt_buffer_aes_ctr(ExmgKeySystemEncryptSession *session, uint8_t *buf, int size) {

    struct AVAESCTR *aes_ctx = av_aes_ctr_alloc();
    av_aes_ctr_init(aes_ctx, &session->exmg_aes_key);
    av_aes_ctr_set_iv(aes_ctx, &session->exmg_aes_iv);
    uint8_t *src_buf = (uint8_t*) malloc(size);
    memcpy(src_buf, buf, size);
    av_aes_ctr_crypt(aes_ctx, buf, src_buf, size);
    free(src_buf);
    av_aes_ctr_free(aes_ctx);
}

static void exmg_key_message_queue_worker(MOVMuxContext* mov)
{
    unsigned int delay = EXMG_KEY_QUEUE_WORKER_POLL * 1000000;
    while(1) {
        //av_log(mov, AV_LOG_VERBOSE, "EXMG key-system worker job\n");
        exmg_key_message_queue_pop(mov);
        // reschedule
        av_usleep(delay);
    }
}

static void exmg_key_system_init(ExmgKeySystemEncryptSession **session_ptr, MOVMuxContext *parent) {
    ExmgKeySystemEncryptSession *session = *session_ptr = malloc(sizeof(ExmgKeySystemEncryptSession));
    memset(session, 0, sizeof(ExmgKeySystemEncryptSession));

    char* message_send_delay = getenv("FF_EXMG_MESSAGE_SEND_DELAY");
    if (message_send_delay != NULL) {
        session->message_send_delay_secs = strtof(message_send_delay, NULL);
    } else {
        session->message_send_delay_secs = EXMG_MESSAGE_SEND_DELAY;
    }

    session->exmg_key_id_counter = 0;
    session->exmg_messages_queue_push_idx = 0;
    session->exmg_messages_queue_pop_idx = -1;

    memset(&session->exmg_messages_queue, 0, sizeof(session->exmg_messages_queue));

    ff_mutex_init(&session->exmg_queue_lock, NULL);
    pthread_create(&session->exmg_queue_worker, NULL, exmg_key_message_queue_worker, parent);

    av_log(parent, AV_LOG_INFO,
        "Initialized EMXG key-system encrypt context. Send-delay=%f [s]\n",
        session->message_send_delay_secs
    );
}
