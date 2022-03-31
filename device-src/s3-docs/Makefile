

SRCS = \
    s3-api-mode-append.puml \
    s3-api-mode-read.puml \
    s3-api-mode-write.puml \
    s3-start-file.puml \
    s3-components.puml

all: $(patsubst %.puml,%.svg,$(SRCS))

%.svg: %.puml
	plantuml -v -o . -tsvg $^


