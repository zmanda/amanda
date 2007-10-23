# Makefile for Amanda wrapper programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)

SCRIPTS_PERL =	\
	amgtar \
	generic-dumper

dumperdir = $(DUMPER_DIR)
dumper_SCRIPTS = $(SCRIPTS_PERL)

install-data-hook:
	@list="$(dumper_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(dumperdir)/`echo $$p|sed '$(transform)'`; \
		echo chown \"$(BINARY_OWNER)\" $$pa;	\
		chown "$(BINARY_OWNER)" $$pa;		\
		echo chgrp \"$(SETUID_GROUP)\" $$pa;	\
		chgrp "$(SETUID_GROUP)" $$pa;		\
	done

