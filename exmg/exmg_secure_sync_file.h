#pragma once

#include <string.h>

#include "libavutil/log.h"

#include "exmg_secure_sync_structs.h"

/**
 * WARN: Will not perform fs operation atomically on WIN32 ! Intermediate state where fname link
 *       does not exist anymore may appear. Safe on Linux/Unix (POSIX compatible).
 *
 * */
static int exmg_secure_sync_file_trim_leading_lines(const char* fname, int nb_of_lines)
{
    if (nb_of_lines <= 0) return 1;

    static const char tmp_ext[] = ".tmp";

    FILE *f_in;
    FILE *f_out;

    char temp_filename[strlen(fname) + sizeof(tmp_ext)];
    strcpy(temp_filename, fname);
    strcat(temp_filename, tmp_ext);

    f_in = fopen(fname, "r");
    if (!f_in)
    {
        av_log(NULL, AV_LOG_ERROR, "File not found or unable to open the input file: %s\n", fname);
        return 0;
    }

    // open the temporary file in write mode
    f_out = fopen(temp_filename, "w");
    if (!f_out)
    {
        av_log(NULL, AV_LOG_ERROR, "Unable to open a temporary file to write: %s\n", temp_filename);
        fclose(f_in);
        return 0;
    }

    char line_buf[1024];
    int line_buf_size = sizeof(line_buf);

    // copy all contents to the temporary file except the specific line
    for (int line_idx = 0; !feof(f_in); line_idx++)
    {
        if (fgets(line_buf, line_buf_size, f_in) == NULL) break;
        /* skip the line at given line number */
        if (line_idx < nb_of_lines) continue;
        fprintf(f_out, "%s", line_buf);
    }

    fclose(f_in);
    fclose(f_out);

    // rename the temporary file to original name
    // First: attempt a posix/unix "atomic move" first (ideal happy path, as otherwise
    // we can have state where `fname` path does not exist anymore at all).
    // Second: `rename` on an existing path is safe to fail on Windows, we then fallback
    // to removing the existing link first, then finally rename. Undesired
    // state would be allowed to exist between these two calls in this case.
    // see https://stackoverflow.com/questions/167414/is-an-atomic-file-rename-with-overwrite-possible-on-windows/60963667#60963667
    // and specifically https://stackoverflow.com/a/60963667/589493 for an uptodate 2020 answer.
    if (rename(temp_filename, fname) != 0) {
        remove(fname);
        rename(temp_filename, fname);
    }

    return 1;
}

/**
 * Filesystem convenience layer around POSIX
 * to write the file straight forward given path and message data
 * in append mode or plain "w" optionnaly.
 * Data will be "println'd" to the the FILE as chars (%s format).
 * If in append mode, trim_lines will remove number of lines
 * from beginning.
 * Any negative trim_lines value will result in no limitation at all.
 */
static int exmg_secure_sync_file_write_line(const char *path, const uint8_t *data, int append, int trim_lines)
{
    av_log(NULL, AV_LOG_INFO, "exmg_secure_sync_write_file: %s\n", path);

    FILE *f;
    if (append) {
        exmg_secure_sync_file_trim_leading_lines(path, trim_lines);
        f = fopen(path, "a");
    } else {
        f = fopen(path, "w");
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

    res = exmg_secure_sync_file_write_line(filename, message_buffer, 0, 0);
    if (!res) {
        av_log(session->mov, AV_LOG_ERROR, "Fatal error writing key-message!\n");
        exit(1);
    }

    int trim_lines = session->key_index_max_window >= 0 && (((int) session->key_index_counter) > session->key_index_max_window) ? 1 : 0;

    res = exmg_secure_sync_file_write_line(index_path, (uint8_t*) filename, 1, trim_lines);
    if (!res) {
        av_log(session->mov, AV_LOG_ERROR, "Fatal error writing key-message!\n");
        exit(1);
    }

    free(index_path);
    free(filename);
}
