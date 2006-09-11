# Makefile for Amanda tape changer programs.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src   \
		-I$(top_srcdir)/server-src   \
		-I$(top_srcdir)/tape-src

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

LIB_EXTENSION = la

if WANT_CHIO_SCSI
CHIO_SCSI = chg-scsi-chio
endif

if WANT_CHG_SCSI
CHG_SCSI = chg-scsi
endif

libexec_PROGRAMS =	$(CHG_SCSI)
EXTRA_PROGRAMS =	$(CHIO_SCSI)

libexec_SCRIPTS =	chg-manual	chg-multi	chg-mtx		\
			chg-rth		chg-chs		chg-chio	\
			chg-zd-mtx	chg-juke	chg-rait	\
			chg-null	chg-mcutil	chg-disk	\
			chg-iomega

CLEANFILES = $(libexec_SCRIPTS)

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.$(LIB_EXTENSION) \
	../server-src/libamserver.$(LIB_EXTENSION) \
	../tape-src/libamtape.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION)   \
	$(READLINE_LIBS)

SUFFIXES =		.pl .sh

chg_scsi_CSRC = chg-scsi.c scsi-aix.c scsi-changer-driver.c		\
		scsi-hpux_new.c scsi-irix.c scsi-linux.c scsi-solaris.c	\
		scsi-bsd.c scsi-cam.c sense.c 
chg_scsi_SOURCES = libscsi.h scsi-defs.h $(chg_scsi_CSRC)

chg_scsi_chio_CSRC = chg-scsi-chio.c scsi-hpux.c scsi-chio.c
chg_scsi_chio_SOURCES = libscsi.h $(chg_scsi_chio_CSRC)

EXTRA_DIST = scsi-proto.c

.pl:
			cat $< > $@
			chmod a+x $@
			-test -z "$(PERL)" || $(PERL) -c $@

.sh:
			cat $< > $@
			chmod a+x $@

install-exec-hook:
	@list="$(sbin_PROGRAMS) $(sbin_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(sbindir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
	done
	@list="$(libexec_PROGRAMS) $(libexec_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(libexecdir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
	done

lint:
	@ for p in $(libexec_PROGRAMS) $(EXTRA_PROGRAMS); do			\
		f="$$p.c $(libamandad_la_SOURCES)";				\
		(cd ../common-src; make listlibsrc);				\
		f="$$f "`cat ../common-src/listlibsrc.output`;			\
		(cd ../server-src; make listlibsrc);				\
		f="$$f "`cat ../server-src/listlibsrc.output`;			\
		(cd ../tape-src; make listlibsrc);				\
		f="$$f "`cat ../tape-src/listlibsrc.output`;			\
		echo $(LINT) $$f;						\
		$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I../config	\
		    $(INCLUDES) $$f;						\
		if [ $$? -ne 0 ]; then						\
		    exit 1;							\
		fi;								\
	done;									\
        exit 0
