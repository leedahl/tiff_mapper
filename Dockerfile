FROM redhat/ubi9

ENV HOME=/root
ADD dist/ $HOME/src/

RUN dnf install -y python3.12 python3.12-pip && \
    pip3.12 install -U pip && \
    cd $HOME/src && \
    pip3.12 install $(ls geoparquet_library-*.tar.gz) && \
    pip3.12 install $(ls tiff_mapper-*.tar.gz) && \
    cd $HOME && \
    rm -rf $HOME/src

ENTRYPOINT ["tiff_mapper", "mapper"]