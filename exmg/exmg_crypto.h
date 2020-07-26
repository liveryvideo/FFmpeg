#pragma once

#include "exmg_secure_sync_structs.h"

static void exmg_encrypt_buffer_aes_ctr(ExmgSecureSyncEncSession *session, uint8_t *buf, int size) {

    // read short-key data
    uint32_t media_encrypt_key;
    uint32_t media_encrypt_iv;
    memcpy(&media_encrypt_key, &session->aes_key, sizeof(media_encrypt_key));
    memcpy(&media_encrypt_iv, &session->aes_iv, sizeof(media_encrypt_iv));

    av_log(session->mov, AV_LOG_VERBOSE, "Using key/iv pair to encrypt buffer (%d bytes): %u (0x%08X) / %u (0x%08X)\n",
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