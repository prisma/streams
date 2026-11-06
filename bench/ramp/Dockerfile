# The ramp rig's image: Linux binaries built on the host by build.sh
# (cargo zigbuild, glibc 2.39), so the image build compiles nothing.
FROM ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY bin/streams-slate bin/s3lite bin/bench /usr/local/bin/
