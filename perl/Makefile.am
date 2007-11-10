##
## Prepare for SWIGging
## 

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

# set compilation flags for all compiles in this subdirectory (including libamglue)
AM_CFLAGS = $(PERL_EXT_CFLAGS)

# -DSWIG is included in CFLAGS so header files can conditionally compile
# segments that will confuse Perl compiles
AM_CFLAGS += -DSWIG

# Appropriate INCLUDES depends on which components (server or client) are 
# being built
INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src \
		-I$(top_srcdir)/perl/amglue \
		-I$(top_srcdir)/gnulib

if WANT_SERVER
INCLUDES += -I$(top_srcdir)/device-src \
	    -I$(top_srcdir)/server-src
endif

if WANT_CLIENT
INCLUDES += -I$(top_srcdir)/client-src
endif

if WANT_RESTORE
INCLUDES += -I$(top_srcdir)/restore-src
endif

if WANT_RECOVER
INCLUDES += -I$(top_srcdir)/recover-src
endif

# (amplot has a conditional, but doesn't have any header files)

##
## libamglue -- helper functions for gluing amanda to perl
##

amlib_LTLIBRARIES = libamglue.la
libamglue_la_SOURCES = \
	amglue/amglue.h \
	amglue/ghashtable.c
EXTRA_DIST += $(libamglue_la_SOURCES)

##
## Plain old perl modules
##

# (Amandadir/Amanda_DATA are also used by the SWIG modules)
Amandadir=$(amperldir)/Amanda
nodist_Amanda_DATA =

##
## SWIG-generated packages
##

# Tell libtool to not bother versioning the libs we build, since perl won't
# pay attention to such things, anyway
PERL_EXT_LDFLAGS += -avoid-version

# And tell libtool that we're building modules
PERL_EXT_LDFLAGS += -module

# list our SWIG libraries. 
AMGLUE_SWG = \
	amglue/glib.swg \
	amglue/constants.swg \
	amglue/exports.swg
EXTRA_DIST += $(AMGLUE_SWG)

# Suffix rules.  These are parallel-build compatible, but may cause
# strange behavior if there are other .c files in this directory.  The
# 'rm -f $@' is needed because SWIG sometimes fails to remove its output
# file in the event of a syntax error.  .i files are pre-processor output;
# they are only used during development.
SWIG_OPTIONS=-perl5 -proxy -I$(srcdir)
%.c : %.swg $(AMGLUE_SWG)
	$(mkdir_p) `dirname $@`
	swig $(SWIG_OPTIONS) -o $@ $(top_srcdir)/perl/$< || { rm -f $@; false; }
%.pm : %.c
	@echo "$@ was produced as a side-effect of creating $<"
%.i : %.swg $(AMGLUE_SWG)
	swig $(SWIG_OPTIONS) -E $(top_srcdir)/perl/$< >$@

if WANT_SERVER
# PACKAGE: Amanda::Device
libDevicedir = $(amperldir)/auto/Amanda/Device
libDevice_LTLIBRARIES = libDevice.la
nodist_libDevice_la_SOURCES = Amanda/Device.c $(AMGLUE_SWG)
libDevice_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libDevice_la_LIBADD = libamglue.la \
	$(top_builddir)/device-src/libamdevice.la \
	$(top_builddir)/common-src/libamanda.la
nodist_Amanda_DATA += Amanda/Device.pm
EXTRA_DIST += Amanda/Device.swg
DISTCLEANFILES += Amanda/Device.c Amanda/Device.pm

# PACKAGE: Amanda::Logfile
libLogfiledir = $(amperldir)/auto/Amanda/Logfile
libLogfile_LTLIBRARIES = libLogfile.la
nodist_libLogfile_la_SOURCES = Amanda/Logfile.c $(AMGLUE_SWG)
libLogfile_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libLogfile_la_LIBADD = libamglue.la \
	$(top_builddir)/server-src/libamserver.la \
	$(top_builddir)/common-src/libamanda.la
nodist_Amanda_DATA += Amanda/Logfile.pm
EXTRA_DIST += Amanda/Logfile.swg
DISTCLEANFILES += Amanda/Logfile.c Amanda/Logfile.pm

# PACKAGE: Amanda::Cmdline
libCmdlinedir = $(amperldir)/auto/Amanda/Cmdline
libCmdline_LTLIBRARIES = libCmdline.la
nodist_libCmdline_la_SOURCES = Amanda/Cmdline.c $(AMGLUE_SWG)
libCmdline_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libCmdline_la_LIBADD = libamglue.la \
	$(top_builddir)/server-src/libamserver.la \
	$(top_builddir)/common-src/libamanda.la
nodist_Amanda_DATA += Amanda/Cmdline.pm
EXTRA_DIST += Amanda/Cmdline.swg
DISTCLEANFILES += Amanda/Cmdline.c Amanda/Cmdline.pm
endif

# PACKAGE: Amanda::Debug
libDebugdir = $(amperldir)/auto/Amanda/Debug
libDebug_LTLIBRARIES = libDebug.la
nodist_libDebug_la_SOURCES = Amanda/Debug.c $(AMGLUE_SWG)
libDebug_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libDebug_la_LIBADD = \
	$(top_builddir)/common-src/libamanda.la
nodist_Amanda_DATA += Amanda/Debug.pm
EXTRA_DIST += Amanda/Debug.swg
DISTCLEANFILES += Amanda/Debug.c Amanda/Debug.pm

# PACKAGE: Amanda::Config
libConfigdir = $(amperldir)/auto/Amanda/Config
libConfig_LTLIBRARIES = libConfig.la
nodist_libConfig_la_SOURCES = Amanda/Config.c $(AMGLUE_SWG)
libConfig_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libConfig_la_LIBADD = libamglue.la \
	$(top_builddir)/common-src/libamanda.la
nodist_Amanda_DATA += Amanda/Config.pm
EXTRA_DIST += Amanda/Config.swg
DISTCLEANFILES += Amanda/Config.c Amanda/Config.pm
