FROM ubuntu:18.04
# Set the working directory to /app
WORKDIR /app

RUN \
    apt-get update && \
    apt-get install software-properties-common -y && \
    add-apt-repository universe && \
    add-apt-repository ppa:xorg-edgers/ppa && \
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

# CircleCI
RUN apt-get install -y \
    git \
    openssh-client \
    ca-certificates \
    tar \
    zip

# Setup vmaf
RUN cd /tmp;git clone https://github.com/Netflix/vmaf.git;cd vmaf/ptools;make;cd ../wrapper;make -j8;cd ..;sudo make install;cd /app

# Copy the current directory contents into the container at /app
COPY . /app

RUN make clean

RUN ./configure  \
    --enable-openssl \
    --enable-version3 \
    --enable-static \
    --disable-sdl2 \
    --disable-debug \
    --disable-ffplay \
    --disable-libxcb \
    --disable-libxcb-shm \
    --disable-indev=sndio \
    --disable-outdev=sndio \
    --disable-outdev=xv \
    --enable-libx264 \
    --enable-libpulse \
    --enable-vaapi \
    --enable-libfreetype \
    --enable-fontconfig \
    --enable-libvmaf \
    --extra-libs="-lpthread -lm" \
    --pkg-config-flags=--static

RUN make -j8
