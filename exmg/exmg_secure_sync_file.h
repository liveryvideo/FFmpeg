#pragma once

#include <string.h>

#include "libavutil/log.h"

#include "exmg_secure_sync_structs.h"

/**
 * POSIX FS convenience layer to write the file straight forward
 * given filename and message string.
 */
static int exmg_secure_sync_write_file(const char *filename, const uint8_t *data, int append)
{
    av_log(NULL, AV_LOG_INFO, "exmg_secure_sync_write_file: %s\n", filename);

    FILE *f;
    if (append) {
        f = fopen(filename, "a");
    } else {
        f = fopen(filename, "w");
    }

    if (f == NULL)
    {
        av_log(NULL, AV_LOG_ERROR, "exmg_secure_sync_write_file: failed to open file!\n");
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
 * This function will then directly call to `exmg_secure_sync_write_file` with its result.
 */
static void exmg_secure_sync_publish_key_message_to_file(ExmgSecureSyncEncSession* session, 
    uint8_t* message_buffer, MOVTrack* track, int64_t message_media_time) {

    #define KEY_MESSAGE_FILENAME_TEMPLATE "exmg_key_%s_%d_%ld.json"

    const char *base_dir = session->fs_pub_basepath;

    const char* media_type = av_get_media_type_string(track->par->codec_type);

    char* index_path = (char*) malloc(strlen(base_dir) + 64);
    strcpy(index_path, base_dir);
    strcat(index_path, "exmg_key_index_");
    strcat(index_path, media_type); // we open one file per media-index to avoid concurrent access across the various movenc instances/thread-loops

    char *filename_template = (char*) malloc(strlen(base_dir) + strlen(KEY_MESSAGE_FILENAME_TEMPLATE) + 1);
    strcpy(filename_template, base_dir);
    strcat(filename_template, KEY_MESSAGE_FILENAME_TEMPLATE);

    int filename_len = strlen(filename_template) + 256; // we add 256 chars of leeway for the semantics we print in
    char *filename = (char*) malloc(filename_len + 1);
    int res = snprintf(filename, filename_len, filename_template,
        media_type,
        track->track_id,
        message_media_time
    );

    free(filename_template);

    if (res <= 0 || res >= filename_len){
        av_log(session->mov, AV_LOG_ERROR, "Fatal error writing string, snprintf result value: %d\n", res);
        exit(1);
    }

    res = exmg_secure_sync_write_file(filename, message_buffer, 0);
    if (!res) {
        av_log(session->mov, AV_LOG_ERROR, "Fatal error writing key-message!\n");
        exit(1);
    }

    res = exmg_secure_sync_write_file(index_path, (uint8_t*) filename, 1);
    if (!res) {
        av_log(session->mov, AV_LOG_ERROR, "Fatal error writing key-message!\n");
        exit(1);
    }

    free(index_path);
    free(filename);
}