#!/bin/bash

# Bash setup (exit on error)
set -e

export stream_id=664379
export segment_size_in_seconds=1
export window_size_in_segments=20
export window_extra_segments=31536000
export frame_rate_num=30000
export frame_rate_den=1000
export input_width=640
export input_height=480
export video_bitrate="800k"
export audio_bitrate="128k"
export output_resolution="640x480"
export input="Spring.mp4"
export event_name="stephan"

sub_folder="$(date +%s)"

#export output="https://p-ep$stream_id.i.akamaientrypoint.net/cmaf/$stream_id/$event_name"

rm -Rf ../dash-out-test/*
mkdir -p ../dash-out-test/$sub_folder

export output=../dash-out-test

export FF_EXMG_KEYS_MQTT=1

export FF_EXMG_KEY_ENCRYPT_ON=1 # any non-empty string true
export FF_EXMG_KEY_FILE_OUT=$output/ # ending slash is mandatory (or empty string "")
export FF_EXMG_KEY_MESSAGE_SEND_DELAY="1" # seconds (float)
export FF_EXMG_KEY_SCOPE_NB_OF_FRAGMENTS="30" # amount (int)

echo "\n$output\n"

export log_level="info" # quiet / error / debug / verbose

./ffmpeg \
       -loglevel repeat+level+$log_level \
       -re -i $input \
       -flags +global_header \
       -r $frame_rate_num/$frame_rate_den \
       -af aresample=async=1 \
       -c:v libx264 \
       -preset medium \
       -b:v $video_bitrate \
       -s $output_resolution \
       -pix_fmt yuv420p \
       -sc_threshold 0 \
       -force_key_frames "expr:gte(t,n_forced*"$segment_size_in_seconds")" \
       -bf 0 \
       -x264opts scenecut=-1:rc_lookahead=0 \
       -c:a aac \
       -b:a $audio_bitrate \
       -seg_duration $segment_size_in_seconds \
       -use_timeline 0 \
       -streaming 1 \
       -index_correction 1 \
       -http_persistent 1 \
       -ignore_io_errors 1\
       -media_seg_name  $sub_folder'/segment_$RepresentationID$-$Number%05d$.m4s' \
       -init_seg_name  $sub_folder'/init_$RepresentationID$.m4s' \
       -hls_playlist 1 \
       -window_size $window_size_in_segments \
       -extra_window_size $window_extra_segments \
       $output/out.mpd

