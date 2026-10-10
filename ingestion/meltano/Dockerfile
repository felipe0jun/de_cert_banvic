FROM meltano/meltano:v4.4.1-python3.12

WORKDIR /project

COPY meltano.yml ./
COPY plugins/ ./plugins/

RUN meltano install --clean

VOLUME ["/project/data"]

ENTRYPOINT ["meltano"]
CMD ["--help"]
