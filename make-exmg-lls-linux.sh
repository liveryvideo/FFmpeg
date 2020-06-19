#!/bin/bash

# enable-version3 enables LGPL licence version 3
# build result written to the output directory so we can easily link against it
./configure \
  --enable-openssl \
  --enable-version3 \
  --disable-sdl2 \
  --enable-debug \
  --disable-ffplay \
  --disable-libxcb \
  --disable-libxcb-shm \
  --disable-indev=sndio \
  --disable-outdev=sndio \
  --disable-outdev=xv \
  --enable-libpulse \
  --enable-libvmaf \
  --pkg-config-flags=--static \
  --prefix="output"

make -j4
cp ffmpeg_g ../playtrivia-video-tests/ffmpeg-exmg-lls-linux
cp ffmpeg_g ~/go/src/bitbucket.org/exmachina/video-encoder/assets/ffmpeg-exmg-lls-linux

