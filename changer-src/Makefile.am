# Makefile for Amanda tape changer programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src   \
		-I$(top_srcdir)/device-src   \
		-I$(top_srcdir)/gnulib

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

if WANT_CHG_SCSI_CHIO
CHIO_SCSI = chg-scsi-chio
endif

if WANT_CHG_SCSI
CHG_SCSI = chg-scsi
endif

amlibexec_PROGRAMS =	$(CHG_SCSI)
EXTRA_PROGRAMS =	$(CHIO_SCSI)

SCRIPTS_PERL = \
		chg-glue \
		chg-chio \
		chg-iomega \
		chg-rth

SCRIPTS_SHELL = \
		chg-manual \
		chg-multi \
		chg-mtx \
		chg-chs \
		chg-zd-mtx \
		chg-juke \
		chg-mcutil \
		chg-disk

SCRIPTS_INCLUDE = \
		chg-lib.sh
EXTRA_DIST += chg-lib.sh.in

amlibexec_SCRIPTS = $(SCRIPTS_PERL) $(SCRIPTS_SHELL)
amlibexec_DATA = $(SCRIPTS_INCLUDE)

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.la \
	../common-src/libamanda.la \
	../device-src/libamdevice.la \
	../gnulib/libgnu.la \
	$(READLINE_LIBS)

chg_scsi_CSRC = chg-scsi.c scsi-changer-driver.c sense.c 
if WANT_SCSI_AIX
chg_scsi_CSRC += scsi-aix.c
endif
if WANT_SCSI_HPUX_NEW
chg_scsi_CSRC += scsi-hpux_new.c
endif
if WANT_SCSI_IRIX
chg_scsi_CSRC += scsi-irix.c
endif
if WANT_SCSI_LINUX
chg_scsi_CSRC += scsi-linux.c
endif
if WANT_SCSI_SOLARIS
chg_scsi_CSRC += scsi-solaris.c
endif
if WANT_SCSI_BSD
chg_scsi_CSRC += scsi-bsd.c
endif
if WANT_SCSI_CAM
chg_scsi_CSRC += scsi-cam.c
endif

chg_scsi_SOURCES = libscsi.h scsi-defs.h $(chg_scsi_CSRC)

chg_scsi_chio_CSRC = chg-scsi-chio.c
if WANT_SCSI_HPUX
chg_scsi_chio_CSRC += scsi-hpux.c
endif
if WANT_SCSI_CHIO
chg_scsi_chio_CSRC += scsi-chio.c
endif
chg_scsi_chio_SOURCES = libscsi.h $(chg_scsi_chio_CSRC)

EXTRA_DIST += scsi-proto.c

INSTALLPERMS_exec = \
	chown=amanda \
	dest=$(sbindir) $(sbin_PROGRAMS) \
	dest=$(amlibexecdir) $(amlibexec_PROGRAMS) 

INSTALLPERMS_data = \
	chown=amanda \
	dest=$(sbindir) $(sbin_SCRIPTS) \
	dest=$(amlibexecdir) $(amlibexec_SCRIPTS) $(amlibexec_DATA)

lint:
	@ for p in $(amlibexec_PROGRAMS) $(EXTRA_PROGRAMS); do			\
		p=`basename $$p $(EXEEXT)`;					\
		f="$$p.c $(libamandad_la_SOURCES)";				\
		(cd ../common-src; make listlibsrc);				\
		f="$$f "`cat ../common-src/listlibsrc.output`;			\
		(cd ../server-src; make listlibsrc);				\
		f="$$f "`cat ../server-src/listlibsrc.output`;			\
		echo $(LINT) $$f;						\
		$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I$(top_builddir)/config	\
		    $(INCLUDES) $$f;						\
		if [ $$? -ne 0 ]; then						\
		    exit 1;							\
		fi;								\
	done;									\
        exit 0
