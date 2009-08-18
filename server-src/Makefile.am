# Makefile for Amanda server programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src   \
		-I$(top_srcdir)/restore-src  \
		-I$(top_srcdir)/amandad-src  \
		-I$(top_srcdir)/device-src   \
		-I$(top_srcdir)/xfer-src   \
		-I$(top_srcdir)/gnulib

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

amlib_LTLIBRARIES = 	libamserver.la

sbin_PROGRAMS =		amadmin		amcheck		\
			amflush		\
			amservice	amreport

amlibexec_PROGRAMS =	amindexd	amlogroll	amtrmidx	\
			amtrmlog	driver		dumper		\
			planner		amcleanupdisk	\
			chunker

amlibexec_SCRIPTS_PERL = \
	    taper \
	    amcheck-device

sbin_SCRIPTS_PERL = \
	    amaddclient \
	    amoverview \
	    amserverconfig \
	    amtoc \
	    amcheckdump \
	    amcleanup \
	    amrmtape \
	    amlabel \
	    amtape \
	    amvault \
	    amstatus
if WANT_RESTORE
sbin_SCRIPTS_PERL += \
	    amrestore
endif

sbin_SCRIPTS_SHELL = \
	    amcheckdb \
	    amdump

SCRIPTS_PERL = $(sbin_SCRIPTS_PERL) $(amlibexec_SCRIPTS_PERL)
SCRIPTS_SHELL = $(sbin_SCRIPTS_SHELL) $(amlibexec_SCRIPTS_SHELL)
sbin_SCRIPTS = $(sbin_SCRIPTS_PERL) $(sbin_SCRIPTS_SHELL)
amlibexec_SCRIPTS = $(amlibexec_SCRIPTS_PERL) $(amlibexec_SCRIPTS_SHELL)

INSTALLPERMS_exec = \
	dest=$(sbindir) chown=amanda chmod= \
		$(sbin_PROGRAMS) \
	chown=root chmod=04750 \
		amcheck amservice \
	dest=$(amlibexecdir) chown=amanda chmod= \
		$(amlibexec_PROGRAMS) \
		$(amlibexec_SCRIPTS) \
	chown=root chmod=04750 \
		dumper planner

INSTALLPERMS_data = \
	dest=$(sbindir) chown=amanda chmod= \
		$(sbin_SCRIPTS) \
	dest=$(amlibexecdir) chown=amanda chmod= \
		$(amlibexec_SCRIPTS)

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.la   \
	libamserver.la               \
	../device-src/libamdevice.la     \
	../common-src/libamanda.la   \
        $(READLINE_LIBS)

libamserver_la_SOURCES=	amindex.c	changer.c			\
			diskfile.c	driverio.c	cmdline.c  \
			holding.c	infofile.c	logfile.c	\
			tapefile.c	find.c		server_util.c   \
                        xfer-source-holding.c

libamserver_la_LDFLAGS= -release $(VERSION)
libamserver_la_LIBADD= ../device-src/libamdevice.la \
		       ../common-src/libamanda.la

amindexd_LDADD = $(LDADD) \
	../amandad-src/libamandad.la

# there are used for testing only:
TEST_PROGS = diskfile infofile

EXTRA_PROGRAMS =	$(TEST_PROGS)

CLEANFILES += *.test.c

amindexd_CSRC =		amindexd.c	disk_history.c	list_dir.c
amindexd_SOURCES =	disk_history.h	list_dir.h	$(amindexd_CSRC)

amreport_SOURCES =	reporter.c

noinst_HEADERS = 	amindex.h	changer.h	cmdline.h	\
			diskfile.h	driverio.h	\
			holding.h	infofile.h	logfile.h	\
			tapefile.h	find.h		server_util.h	\
			xfer-server.h

lint:
	@ for p in $(amlibexec_PROGRAMS) $(sbin_PROGRAMS); do			\
		p=`basename $$p $(EXEEXT)`;					\
		if [ $$p = "amindexd" ]; then					\
			s="$(amindexd_CSRC)";					\
		elif [ $$p = "amreport" ]; then					\
			s="$(amreport_SOURCES)";				\
		else								\
			s=$$p.c;						\
		fi; 								\
		f="$$s $(libamserver_la_SOURCES)";				\
		(cd ../common-src; make listlibsrc);				\
		f="$$f "`cat ../common-src/listlibsrc.output`;			\
		echo $(LINT) $$f;						\
		$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I$(top_builddir)/config	\
		    $(INCLUDES) $$f;						\
		if [ $$? -ne 0 ]; then						\
		    exit 1;							\
		fi;								\
	done;									\
        exit 0

listlibsrc:
	@ for p in $(libamserver_la_SOURCES); do		\
		listlibsrcs="$$listlibsrcs `pwd`/$$p";		\
	done;							\
	echo $$listlibsrcs >listlibsrc.output


diskfile_SOURCES = diskfile.test.c
infofile_SOURCES = infofile.test.c

%.test.c: $(srcdir)/%.c
	echo '#define TEST' >$@
	echo '#include "$<"' >>$@
