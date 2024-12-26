FROM redhat/ubi9-minimal AS base

FROM redhat/ubi9 AS builder
COPY --from=base / /app
RUN dnf install -y --installroot=/app --releasever=latest python3.12 python3.12-pip

FROM scratch AS app
COPY --from=builder /app /

ENV HOME=/root
ADD dist/ $HOME/src/

RUN pip3.12 install --root-user-action ignore -U pip && \
    cd $HOME/src && \
    pip3.12 install --root-user-action ignore $(ls geoparquet_library-*.tar.gz) && \
    pip3.12 install --root-user-action ignore $(ls tiff_mapper-*.tar.gz)

FROM redhat/ubi9-minimal AS copy
COPY --from=app / /temp
RUN rm -rf /temp/tmp /temp/dev /temp/usr/bin/newgidmap /temp/usr/bin/newuidmap

FROM  scratch AS image
COPY --from=copy /temp /

ENTRYPOINT ["tiff_mapper", "mapper"]