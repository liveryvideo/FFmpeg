#pragma once

#include <stdio.h>
#include <string.h>

#include "exmg_queue.h"
#include "exmg_mqtt.h"

#include "libavformat/movenc.h"

typedef struct ExmgSecureSyncEncSession {
    MOVMuxContext *mov;

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

    int is_dry_run;
    int is_encryption_enabled;

    const char* fs_pub_basepath;
    ExmgMqttPubContext *mqtt_pub_ctx;

} ExmgSecureSyncEncSession;

typedef struct ExmgSecureSyncScope {
    uint8_t *media_key_message;
    int64_t media_time;
} ExmgSecureSyncScope;