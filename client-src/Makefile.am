# Makefile for Amanda client programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

AM_CPPFLAGS =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src   \
		-I$(top_srcdir)/amandad-src  \
		-I$(top_srcdir)/gnulib

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS) $(AMANDA_FILE_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS) $(AS_NEEDED_FLAGS)

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

amlib_LTLIBRARIES =	libamclient.la

amlibexec_PROGRAMS =	noop calcsize killpgrp rundump runtar selfcheck sendbackup senddiscover sendsize

amlibexec_SCRIPTS_SHELL = patch-system
amlibexec_SCRIPTS_PERL = restore

sbin_SCRIPTS_PERL = amdump_client ambackup

SCRIPTS_PERL = $(sbin_SCRIPTS_PERL) $(amlibexec_SCRIPTS_PERL)
SCRIPTS_SHELL = $(amlibexec_SCRIPTS_SHELL)
sbin_SCRIPTS = $(sbin_SCRIPTS_PERL)
amlibexec_SCRIPTS = $(amlibexec_SCRIPTS_SHELL) $(amlibexec_SCRIPTS_PERL)

libamclient_la_SOURCES=	amandates.c		getfsent.c	\
			unctime.c		client_util.c
if WANT_SAMBA
libamclient_la_SOURCES += findpass.c
endif

libamclient_la_LDFLAGS = -release $(VERSION) $(AS_NEEDED_FLAGS)
libamclient_la_LIBADD  = ../common-src/libamanda.la

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.la \
	libamclient.la \
	../amandad-src/libamandad.la \
	../common-src/libamanda.la \
	../gnulib/libgnu.la

# these are used for testing only:
TEST_PROGS = getfsent

EXTRA_PROGRAMS =	$(TEST_PROGS)

CLEANFILES += *.test.c $(SCRIPTS_PERL) $(SCRIPTS_SHELL)
DISTCLEANFILES += config.log


sendbackup_SOURCES = 	sendbackup.c		sendbackup.h	  \
			sendbackup-dump.c	sendbackup-gnutar.c

noinst_HEADERS	= 	amandates.h	getfsent.h	\
			findpass.h	client_util.h
			
if WANT_SETUID_CLIENT
INSTALLPERMS_exec = dest=$(amlibexecdir) chown=root:setuid chmod=04750 \
		calcsize killpgrp rundump runtar
endif

lint:
	@ for p in $(amlibexec_PROGRAMS); do					\
		p=`basename $$p $(EXEEXT)`;					\
		f="$$p.c $(libamclient_la_SOURCES)";				\
		(cd ../common-src; make listlibsrc);				\
		f="$$f "`cat ../common-src/listlibsrc.output`;			\
		echo $(LINT) $$f;						\
		$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I$(top_builddir)/config	\
		    $(AM_CPPFLAGS) $$f;						\
		if [ $$? -ne 0 ]; then						\
		    exit 1;							\
		fi;								\
	done;									\
        exit 0

getfsent_SOURCES = getfsent.test.c

%.test.c: $(srcdir)/%.c
	echo '#define TEST' >$@
	echo '#include "$<"' >>$@
