#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail

apt-get update

apt-get install -y \
    alsa \
    apt-utils \
    automake \
    autoconf \
    build-essential \
    cmake \
    dkms \
    git-core \
    libasound2-dev \
    libass-dev \
    libfreetype6-dev \
    libgl1-mesa-glx \
    libgl1 \
    libpulse-dev \
    libssl-dev \
    libtool \
    libva-dev \
    libva2 \
    libva-drm2 \
    libva-x11-2 \
    libxv1 \
    libx264-152 \
    libx264-dev \
    nasm \
    pkg-config \
    pulseaudio \
    texinfo \
    yasm \
    zlib1g-dev