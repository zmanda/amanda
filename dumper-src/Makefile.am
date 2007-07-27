# Makefile for Amanda wrapper programs.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src

SCRIPTS_PERL =	amgtar generic-dumper
dumper_SCRIPTS = $(SCRIPTS_PERL)
dumperdir =		@DUMPER_DIR@

SUFFIXES =		.sh .pl

.pl:
			cat $< > $@
			chmod a+x $@

.sh:
			cat $< > $@
			chmod a+x $@

DISTCLEANFILES = $(dumper_SCRIPTS)

# automake-style tests

include $(top_srcdir)/config/automake/check-perl.am
CHECK_PERL=$(SCRIPTS_PERL)
check-local: check-perl

install-data-hook:
	@list="$(dumper_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(dumperdir)/`echo $$p|sed '$(transform)'`; \
		echo chown \"$(BINARY_OWNER)\" $$pa;	\
		chown "$(BINARY_OWNER)" $$pa;		\
		echo chgrp \"$(SETUID_GROUP)\" $$pa;	\
		chgrp "$(SETUID_GROUP)" $$pa;		\
	done

