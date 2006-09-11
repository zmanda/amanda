# Makefile for Amanda wrapper programs.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src

dumper_SCRIPTS =	gnutar generic-dumper
dumperdir =		@DUMPER_DIR@

SUFFIXES =		.sh .pl

.pl:
			cat $< > $@
			chmod a+x $@
			-test -z "$(PERL)" || $(PERL) -c $@

.sh:
			cat $< > $@
			chmod a+x $@

DISTCLEANFILES = $(dumper_SCRIPTS)

# these are used for testing only:

install-data-hook:
	@list="$(dumper_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(dumperdir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
	done

