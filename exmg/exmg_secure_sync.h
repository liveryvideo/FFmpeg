/**
 * @author Stephan Hesse <stephan@emliri.com>
 *
 * */

#pragma once

#include <stdio.h>
#include <string.h>

#include "exmg_secure_sync_structs.h"
#include "exmg_mqtt.h"
#include "exmg_crypto.h"
#include "exmg_secure_sync_file.h"

#include "libavutil/time.h"
#include "libavformat/movenc.h"

// hard constants ("should be enough for everyone")
// - or switch to a dynamically allocated queuing/printing
#define EXMG_MESSAGE_BUFFER_SIZE 4096 // bytes, max size of one message
#define EXMG_MESSAGE_QUEUE_SIZE 0xFFFF // queueing capacity, maximum number of message items stored ahead publishing
// hard-constant atm, as in poor-mens thread signaling (see FIXME where used)
#define EXMG_MESSAGE_QUEUE_WORKER_POLL 0.020f // seconds -> default to 50fps to allow maximum needed accuracy

// defaults, for when not set by env
#define EXMG_MESSAGE_SEND_DELAY 10.0f // seconds // overrriden by FF_EXMG_KEY_MESSAGE_SEND_DELAY env var

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

static void exmg_secure_sync_poll_publish_next(MOVMuxContext *mov)
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

    av_log(mov, AV_LOG_DEBUG, "(%s) Next pop'able message media-time: %.3f [s]\n",
        av_get_media_type_string(track->par->codec_type),
        next_popable_message_media_time);

    if (time_diff >= session->message_send_delay_secs) {

        // pop-off queue item we peeked before
        // (this is safe as we kept the mutex locked)
        exmg_queue_pop(session->scope_info_queue);
        ff_mutex_unlock(&session->queue_lock);
        // dipose of queue item
        exmg_secure_sync_scope_dispose(scope_info);

        av_log(mov, AV_LOG_INFO, "(%s) Publishing SecureSync key-message with:\nencryption-scope media-time=%.3f [s]\nat encoding-time=%.3f [s]\neffective key-publish-delay=%.3f [s]\n",
            av_get_media_type_string(track->par->codec_type),
            next_popable_message_media_time,
            media_time_secs,
            time_diff);

        if (session->is_dry_run) {
            av_log(mov, AV_LOG_VERBOSE, "SecureSync dry-run, not publishing anything.\n");
        } else {
            // we need the passed track parameters to create a unique indexable resource name
            exmg_secure_sync_publish_key_message_to_file(session, message_buffer, track, message_media_time);
        }

        free(message_buffer); // free the buffer we malloc'd when put on the queue

    } else {

        av_log(mov, AV_LOG_VERBOSE, "(%s) SecureSync queue not pop'd, media-time difference is: %.3f secs\n",
            av_get_media_type_string(track->par->codec_type),
            time_diff);

        ff_mutex_unlock(&session->queue_lock);
    }
}

/**
 *  Gets called once per every fragment created from the movenc thread.
 *
 * */
static void exmg_secure_sync_on_fragment(ExmgSecureSyncEncSession *session)
{
    MOVMuxContext *mov = session->mov;
    if (mov->flags & FF_MOV_FLAG_DASH == 0) {
        return;
    }
    if (mov->nb_streams > 1) {
        av_log(mov, AV_LOG_ERROR, "SecureSync does not support multiple tracks per DASH fragment! Exiting process.");
        exit(1);
        return;
    }

    MOVTrack* track = &mov->tracks[0];
    if (track == NULL) {
        // this might happen during FFmpeg core shutdown intermediate states,
        // and if we don't abort here cause a seg-fault i.e non-clean process termination.
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
        "(%s) Pushed key-message with scope starting at: %.3f [s] for track-id %d\n",
        av_get_media_type_string(track->par->codec_type),
        key_scope_start_secs,
        track->track_id);

}

static void exmg_key_message_queue_worker(ExmgSecureSyncEncSession *s)
{
    // FIXME: instead of a sleep, we should use cond/wait thread signaling here

    unsigned int delay = EXMG_MESSAGE_QUEUE_WORKER_POLL * 1000000;
    while(1) {
        exmg_secure_sync_poll_publish_next(s->mov);
        // reschedule
        av_usleep(delay);
    }
}

static void exmg_secure_sync_enc_session_init(ExmgSecureSyncEncSession **session_ptr, MOVMuxContext *mov) {
    ExmgSecureSyncEncSession *session = (ExmgSecureSyncEncSession *) malloc(sizeof(ExmgSecureSyncEncSession));

    memset(session, 0, sizeof(ExmgSecureSyncEncSession));

    session->mov = mov;

    session->is_dry_run = getenv("FF_EXMG_SECURE_SYNC_DRY_RUN") != NULL;
    session->is_encryption_enabled = getenv("FF_EXMG_SECURE_SYNC_NO_ENCRYPTION") != NULL;

    session->fs_pub_basepath = getenv("FF_EXMG_SECURE_SYNC_FS_PUB_BASEPATH");

    if (getenv("FF_EXMG_SECURE_SYNC_MQTT_PUB") != NULL) {
        ExmgMqttPubConfig mqtt_config = EXMG_MQTT_PUB_CONFIG_DEFAULT_INIT;
        exmg_mqtt_pub_context_init(&session->mqtt_pub_ctx, mqtt_config);
        exmg_mqtt_pub_connect(session->mqtt_pub_ctx);
    }

    char* message_send_delay = getenv("FF_EXMG_SECURE_SYNC_MESSAGE_SEND_DELAY");
    if (message_send_delay != NULL) {
        session->message_send_delay_secs = strtof(message_send_delay, NULL);
    } else {
        av_log(mov, AV_LOG_WARNING, "Using default value for FF_EXMG_SECURE_SYNC_MESSAGE_SEND_DELAY");
        session->message_send_delay_secs = EXMG_MESSAGE_SEND_DELAY;
    }

    char* fragments_per_key = getenv("FF_EXMG_SECURE_SYNC_FRAGMENTS_PER_KEY");
    if (fragments_per_key != NULL) {
        session->fragments_per_key = (uint32_t) atoi(fragments_per_key);
        if (session->fragments_per_key == 0) {
            session->fragments_per_key = 1;
        }
    } else {
        av_log(mov, AV_LOG_WARNING, "Using default value 1 for FF_EXMG_SECURE_SYNC_FRAGMENTS_PER_KEY");
        session->fragments_per_key = 1;
    }

    session->key_scope_duration = 0;
    session->key_frag_counter = 0;
    session->key_id_counter = 0;

    exmg_queue_init(&session->scope_info_queue, EXMG_MESSAGE_QUEUE_SIZE);

    for (int result = 0
            || ff_mutex_init(&session->queue_lock, NULL)
            || pthread_create(&session->queue_worker, NULL, (void *(*)(void*)) exmg_key_message_queue_worker, (void*) session);
                !result;) {
        av_log(mov, AV_LOG_ERROR, "Mutex/Thread creation returned error-code (%d), failed to launch queue worker", result);
        free(session);
        return;
    }

    av_log(mov, AV_LOG_INFO,
        "Initialized SecureSync encode/encrypt context. Key-Publish-Delay=%0.2f [s]; Fragments/Key=%d\n",
        session->message_send_delay_secs, session->fragments_per_key
    );

    *session_ptr = session;

}

// FIXME: also have deinit function