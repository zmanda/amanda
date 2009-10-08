##
## Prepare for SWIGging
##

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

# add the SWIG symbol to avoid conflicts with perl definitions.
# $AMANDA_WARNING_CFLAGS is omitted because SWIG-generated files tend
# to trigger warnings.
AM_CFLAGS = -DSWIG $(AMANDA_SWIG_PERL_CFLAGS)

# Appropriate INCLUDES depends on which components (server or client) are
# being built
INCLUDES =  \
            -I$(top_builddir)/common-src \
            -I$(top_srcdir)/common-src \
            -I$(top_srcdir)/amandad-src \
            -I$(top_srcdir)/amar-src \
            -I$(top_srcdir)/xfer-src \
            -I$(top_srcdir)/perl/amglue \
            -I$(top_srcdir)/gnulib \
            $(PERL_INC)

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
	amglue/ghashtable.c \
	amglue/bigint.c \
	amglue/source.c \
	amglue/objwrap.c \
	amglue/xferwrap.c \
	amglue/amglue.h
libamglue_la_LDFLAGS = $(PERL_EXT_LDFLAGS) -release $(VERSION)
EXTRA_DIST += $(libamglue_la_SOURCES)

##
## HTML Generation
##

# This isn't a very robust implementation, as it's only meant to make the HTML
# available online as part of the automated compilation process.  It assumes
# that all perl modules were built (not just server or client), and that the
# build directory does not contain any "junk".

# HTML_SOURCE_FILES should contain the .pm files from which the HTML should
# be geneated.  This is just the concatenation of foo_DATA for all $(foodir)
# into which perl modules are installed.
HTML_SOURCE_FILES=
html: make_html amperl.css $(HTML_SOURCE_FILES)
	rm -rf html-tmp/ && mkdir html-tmp/
	cp amperl.css html-tmp/
	$(PERL) make_html html-tmp/ $(HTML_SOURCE_FILES)
	cd html-tmp/ && tar -zcf $(abs_builddir)/html.tar.gz .
	rm -rf html-tmp/
##
## SWIG-generated packages
##

# Tell libtool to not bother versioning the libs we build, since perl won't
# pay attention to such things, anyway.
PERL_EXT_LDFLAGS = -avoid-version

# And add any flags the user has requested; on FreeBSD, this pulls in the
# threading library
PERL_EXT_LDFLAGS += $(PERLEXTLIBS)

# list our SWIG libraries.
AMGLUE_SWG = amglue/amglue.swg \
	amglue/amglue.swg \
	amglue/constants.swg \
	amglue/dumpspecs.swg \
	amglue/exports.swg \
	amglue/filehandles.swg \
	amglue/glib.swg \
	amglue/integers.swg
EXTRA_DIST += $(AMGLUE_SWG)

# SWIG OPTIONS:
# -perl5 -proxy: generate perl code, with proxy (shadow) classes
# -DSWIG_TYPE_TABLE: the name for the global type table; customized
#  to minimize type collisions with other SWIG-generated modules
# -I$(srcdir) use include files relative to this directory
SWIG_OPTIONS=-perl5 -proxy -DSWIG_TYPE_TABLE=SWIG_TYPES_Amanda -I$(srcdir)

# Suffix rules.  These are parallel-build compatible, but may cause
# strange behavior if there are other .c files in this directory.  The
# 'rm -f $@' is needed because SWIG sometimes fails to remove its output
# file in the event of a syntax error.  .i files are pre-processor output;
# they are only used during development.
%.c : %.swg $(AMGLUE_SWG)
	$(mkdir_p) `dirname $@`
	$(SWIG) $(SWIG_OPTIONS) -o $@ $(top_srcdir)/perl/$< || { rm -f $@; false; }
%.pm : %.c
	@echo "$@ was produced as a side-effect of creating $<"
%.i : %.swg $(AMGLUE_SWG)
	$(SWIG) $(SWIG_OPTIONS) -E $(top_srcdir)/perl/$< >$@

Amandadir=$(amperldir)/Amanda
Amanda_DATA =
HTML_SOURCE_FILES += $(Amanda_DATA)

# PACKAGE: Amanda::Archive
libArchivedir = $(amperldir)/auto/Amanda/Archive
libArchive_LTLIBRARIES = libArchive.la
libArchive_la_SOURCES = Amanda/Archive.c $(AMGLUE_SWG)
libArchive_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libArchive_la_LIBADD = libamglue.la \
	$(top_builddir)/amar-src/libamar.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Archive.pm
EXTRA_DIST += Amanda/Archive.swg Amanda/Archive.pm Amanda/Archive.pod
MAINTAINERCLEANFILES += Amanda/Archive.c Amanda/Archive.pm

if WANT_SERVER
if WANT_NDMP
# PACKAGE: Amanda::NDMP
libNDMPdir = $(amperldir)/auto/Amanda/NDMP
libNDMP_LTLIBRARIES = libNDMP.la
libNDMP_la_SOURCES = Amanda/NDMP.c $(AMGLUE_SWG)
libNDMP_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libNDMP_la_LIBADD = libamglue.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/NDMP.pm
MAINTAINERCLEANFILES += Amanda/NDMP.c Amanda/NDMP.pm
endif
endif
EXTRA_DIST += Amanda/NDMP.swg Amanda/NDMP.pm

if WANT_SERVER
# PACKAGE: Amanda::Device
libDevicedir = $(amperldir)/auto/Amanda/Device
libDevice_LTLIBRARIES = libDevice.la
libDevice_la_SOURCES = Amanda/Device.c $(AMGLUE_SWG)
libDevice_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libDevice_la_LIBADD = libamglue.la \
	$(top_builddir)/device-src/libamdevice.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Device.pm
MAINTAINERCLEANFILES += Amanda/Device.c Amanda/Device.pm
endif
EXTRA_DIST += Amanda/Device.swg Amanda/Device.pm Amanda/Device.pod

if WANT_SERVER
# PACKAGE: Amanda::Logfile
libLogfiledir = $(amperldir)/auto/Amanda/Logfile
libLogfile_LTLIBRARIES = libLogfile.la
libLogfile_la_SOURCES = Amanda/Logfile.c $(AMGLUE_SWG)
libLogfile_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libLogfile_la_LIBADD = libamglue.la \
	$(top_builddir)/server-src/libamserver.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Logfile.pm
MAINTAINERCLEANFILES += Amanda/Logfile.c Amanda/Logfile.pm
endif
EXTRA_DIST += Amanda/Logfile.swg Amanda/Logfile.pm Amanda/Logfile.pod

if WANT_SERVER
# PACKAGE: Amanda::Cmdline
libCmdlinedir = $(amperldir)/auto/Amanda/Cmdline
libCmdline_LTLIBRARIES = libCmdline.la
libCmdline_la_SOURCES = Amanda/Cmdline.c $(AMGLUE_SWG)
libCmdline_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libCmdline_la_LIBADD = libamglue.la \
	$(top_builddir)/server-src/libamserver.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Cmdline.pm
MAINTAINERCLEANFILES += Amanda/Cmdline.c Amanda/Cmdline.pm
endif
EXTRA_DIST += Amanda/Cmdline.swg Amanda/Cmdline.pm Amanda/Cmdline.pod

if WANT_SERVER
# PACKAGE: Amanda::Tapelist
libTapelistdir = $(amperldir)/auto/Amanda/Tapelist
libTapelist_LTLIBRARIES = libTapelist.la
libTapelist_la_SOURCES = Amanda/Tapelist.c $(AMGLUE_SWG)
libTapelist_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libTapelist_la_LIBADD = libamglue.la \
	$(top_builddir)/server-src/libamserver.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Tapelist.pm
MAINTAINERCLEANFILES += Amanda/Tapelist.c Amanda/Tapelist.pm
endif
EXTRA_DIST += Amanda/Tapelist.swg Amanda/Tapelist.pm Amanda/Tapelist.pod

if WANT_SERVER
# PACKAGE: Amanda::Disklist
libDisklistdir = $(amperldir)/auto/Amanda/Disklist
libDisklist_LTLIBRARIES = libDisklist.la
libDisklist_la_SOURCES = Amanda/Disklist.c $(AMGLUE_SWG)
libDisklist_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libDisklist_la_LIBADD = libamglue.la \
	$(top_builddir)/server-src/libamserver.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Disklist.pm
MAINTAINERCLEANFILES += Amanda/Disklist.c Amanda/Disklist.pm
endif
EXTRA_DIST += Amanda/Disklist.swg Amanda/Disklist.pm Amanda/Disklist.pod

if WANT_SERVER
# PACKAGE: Amanda::Changer
Amanda_DATA += Amanda/Changer.pm
endif
EXTRA_DIST += Amanda/Changer.pm

if WANT_SERVER
# PACKAGE: Amanda::Changer::*
AmandaChangerdir = $(amperldir)/Amanda/Changer
AmandaChanger_DATA = \
	Amanda/Changer/compat.pm \
	Amanda/Changer/disk.pm \
	Amanda/Changer/null.pm \
	Amanda/Changer/rait.pm \
	Amanda/Changer/robot.pm \
	Amanda/Changer/single.pm
HTML_SOURCE_FILES += $(AmandaChanger_DATA)
endif
EXTRA_DIST += $(AmandaChanger_DATA)

if WANT_SERVER
# PACKAGE: Amanda::Taper::*
AmandaTaperdir = $(amperldir)/Amanda/Taper
AmandaTaper_DATA = \
	Amanda/Taper/Scan.pm \
	Amanda/Taper/Scribe.pm
HTML_SOURCE_FILES += $(AmandaTaper_DATA)
endif
EXTRA_DIST += $(AmandaTaper_DATA)

if WANT_SERVER
# PACKAGE: Amanda::Taper::Scan::*
AmandaTaperScandir = $(amperldir)/Amanda/Taper/Scan
AmandaTaperScan_DATA = \
	Amanda/Taper/Scan/traditional.pm
HTML_SOURCE_FILES += $(AmandaTaperScan_DATA)
endif
EXTRA_DIST += $(AmandaTaperScan_DATA)

# PACKAGE: Amanda::Process
if WANT_SERVER
Amanda_DATA += Amanda/Process.pm
endif
EXTRA_DIST += Amanda/Process.pm

if WANT_SERVER
# PACKAGE: Amanda::DB::*
Amanda_DBdir = $(Amandadir)/DB
Amanda_DB_DATA =
HTML_SOURCE_FILES += $(Amanda_DB_DATA)
endif

if WANT_SERVER
# PACKAGE: Amanda::DB::Catalog
Amanda_DB_DATA += Amanda/DB/Catalog.pm
endif
EXTRA_DIST += Amanda/DB/Catalog.pm

# PACKAGE: Amanda::Xfer
libXferdir = $(amperldir)/auto/Amanda/Xfer
libXfer_LTLIBRARIES = libXfer.la
libXfer_la_SOURCES = Amanda/Xfer.c $(AMGLUE_SWG)
libXfer_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libXfer_la_LIBADD = libamglue.la \
	$(top_builddir)/xfer-src/libamxfer.la
Amanda_DATA += Amanda/Xfer.pm
MAINTAINERCLEANFILES += Amanda/Xfer.c Amanda/Xfer.pm
EXTRA_DIST += Amanda/Xfer.swg Amanda/Xfer.pm Amanda/Xfer.pod

if WANT_SERVER
# PACKAGE: Amanda::XferServer
libXferServerdir = $(amperldir)/auto/Amanda/XferServer
libXferServer_LTLIBRARIES = libXferServer.la
libXferServer_la_SOURCES = Amanda/XferServer.c $(AMGLUE_SWG)
libXferServer_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libXferServer_la_LIBADD = libamglue.la \
	$(top_builddir)/server-src/libamserver.la
Amanda_DATA += Amanda/XferServer.pm
MAINTAINERCLEANFILES += Amanda/XferServer.c Amanda/XferServer.pm
EXTRA_DIST += Amanda/XferServer.swg Amanda/XferServer.pm Amanda/XferServer.pod
endif

if WANT_SERVER
# PACKAGE: Amanda::Report
Amanda_DATA += Amanda/Report.pm
EXTRA_DIST += Amanda/Report.pm
endif

# PACKAGE: Amanda::BigIntCompat
Amanda_DATA += Amanda/BigIntCompat.pm
EXTRA_DIST += Amanda/BigIntCompat.pm

# PACKAGE: Amanda::Debug
libDebugdir = $(amperldir)/auto/Amanda/Debug
libDebug_LTLIBRARIES = libDebug.la
libDebug_la_SOURCES = Amanda/Debug.c $(AMGLUE_SWG)
libDebug_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libDebug_la_LIBADD = libamglue.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Debug.pm
EXTRA_DIST += Amanda/Debug.swg Amanda/Debug.pm Amanda/Debug.pod
MAINTAINERCLEANFILES += Amanda/Debug.c Amanda/Debug.pm

# PACKAGE: Amanda::Config
libConfigdir = $(amperldir)/auto/Amanda/Config
libConfig_LTLIBRARIES = libConfig.la
libConfig_la_SOURCES = Amanda/Config.c $(AMGLUE_SWG)
libConfig_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libConfig_la_LIBADD = libamglue.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Config.pm
EXTRA_DIST += Amanda/Config.swg Amanda/Config.pm Amanda/Config.pod
MAINTAINERCLEANFILES += Amanda/Config.c Amanda/Config.pm

# PACKAGE: Amanda::Util
libUtildir = $(amperldir)/auto/Amanda/Util
libUtil_LTLIBRARIES = libUtil.la
libUtil_la_SOURCES = Amanda/Util.c $(AMGLUE_SWG)
libUtil_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libUtil_la_LIBADD = libamglue.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Util.pm
EXTRA_DIST += Amanda/Util.swg Amanda/Util.pm Amanda/Util.pod
MAINTAINERCLEANFILES += Amanda/Util.c Amanda/Util.pm

# PACKAGE: Amanda::Header
libHeaderdir = $(amperldir)/auto/Amanda/Header
libHeader_LTLIBRARIES = libHeader.la
libHeader_la_SOURCES = Amanda/Header.c $(AMGLUE_SWG)
libHeader_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libHeader_la_LIBADD = libamglue.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Header.pm
EXTRA_DIST += Amanda/Header.swg Amanda/Header.pm Amanda/Header.pod
MAINTAINERCLEANFILES += Amanda/Header.c Amanda/Header.pm

# PACKAGE: Amanda::Tests
libTestsdir = $(amperldir)/auto/Amanda/Tests
libTests_LTLIBRARIES = libTests.la
libTests_la_SOURCES = Amanda/Tests.c $(AMGLUE_SWG)
libTests_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libTests_la_LIBADD = libamglue.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/Tests.pm
EXTRA_DIST += Amanda/Tests.swg Amanda/Tests.pm Amanda/Tests.pod
MAINTAINERCLEANFILES += Amanda/Tests.c Amanda/Tests.pm

# PACKAGE: Amanda::MainLoop
libMainLoopdir = $(amperldir)/auto/Amanda/MainLoop
libMainLoop_LTLIBRARIES = libMainLoop.la
libMainLoop_la_SOURCES = Amanda/MainLoop.c $(AMGLUE_SWG)
libMainLoop_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libMainLoop_la_LIBADD = libamglue.la \
	$(top_builddir)/common-src/libamanda.la
Amanda_DATA += Amanda/MainLoop.pm
EXTRA_DIST += Amanda/MainLoop.swg Amanda/MainLoop.pm Amanda/MainLoop.pod
MAINTAINERCLEANFILES += Amanda/MainLoop.c Amanda/MainLoop.pm

# PACKAGE: Amanda::IPC::*
AmandaIPCdir = $(amperldir)/Amanda/IPC
AmandaIPC_DATA = Amanda/IPC/LineProtocol.pm
HTML_SOURCE_FILES += $(AmandaIPC_DATA)
EXTRA_DIST += $(AmandaIPC_DATA)

# PACKAGE: Amanda::IPC:Binary
libIPCBinarydir = $(amperldir)/auto/Amanda/IPC/Binary
libIPCBinary_LTLIBRARIES = libBinary.la
libBinary_la_SOURCES = Amanda/IPC/Binary.c $(AMGLUE_SWG)
libBinary_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libBinary_la_LIBADD = libamglue.la \
	$(top_builddir)/common-src/libamanda.la
AmandaIPC_DATA += Amanda/IPC/Binary.pm
EXTRA_DIST += Amanda/IPC/Binary.swg Amanda/IPC/Binary.pm Amanda/IPC/Binary.pod
MAINTAINERCLEANFILES += Amanda/IPC/Binary.c Amanda/IPC/Binary.pm

# PACKAGE: Amanda::Paths
Amanda_DATA += Amanda/Paths.pm
EXTRA_DIST += Amanda/Paths.pm.in
DISTCLEANFILES += Amanda/Paths.pm

# PACKAGE: Amanda::Constants
Amanda_DATA += Amanda/Constants.pm
EXTRA_DIST += Amanda/Constants.pm.in
DISTCLEANFILES += Amanda/Constants.pm

# PACKAGE: Amanda::Script_App
Amanda_DATA += Amanda/Script_App.pm
EXTRA_DIST += Amanda/Script_App.pm


if WANT_SERVER
# PACKAGE: Amanda::Holding
Amanda_DATA += Amanda/Holding.pm
endif
EXTRA_DIST += Amanda/Holding.pm

if WANT_CLIENT
# PACKAGE: Amanda::Application
libApplicationdir = $(amperldir)/auto/Amanda/Application
libApplication_LTLIBRARIES = libApplication.la
libApplication_la_SOURCES = Amanda/Application.c $(AMGLUE_SWG)
libApplication_la_LDFLAGS = $(PERL_EXT_LDFLAGS)
libApplication_la_LIBADD = libamglue.la \
	$(top_builddir)/client-src/libamclient.la
Amanda_DATA += Amanda/Application.pm
MAINTAINERCLEANFILES += Amanda/Application.c Amanda/Application.pm
endif
EXTRA_DIST += Amanda/Application.swg Amanda/Application.pm Amanda/Application.pod

# PACKAGE: Amanda::Script
Amanda_DATA += Amanda/Script.pm
EXTRA_DIST += Amanda/Script.pm

if WANT_CLIENT
# PACKAGE: Amanda::Application::*
AmandaApplicationdir = $(amperldir)/Amanda/Application
AmandaApplication_DATA = Amanda/Application/Zfs.pm
HTML_SOURCE_FILES += $(AmandaApplication_DATA)
endif
EXTRA_DIST += Amanda/Application/Zfs.pm

# perl doesn't use the cyg prefix and it doesn't search in the bin
# directory, dll must be copied to their modules directories.
if WANT_CYGWIN_COPY_PERL_DLL
amperldirauto="$(DESTDIR)$(amperldir)/auto/Amanda"
install-data-hook:
	@for cygdll in $(amperldirauto)/bin/cyg*.dll; do \
	    destfname=`echo $$cygdll|sed 's!/bin/cyg\([^.]*\).dll!/\1/\1.dll!'`; \
	   cp "$$cygdll" "$$destfname"; \
	done
endif

check-local:
	@MODULES_TO_CHECK="$(Amanda_DATA) $(Amanda_DB_DATA) $(AmandaChanger_DATA) $(AmandaApplication_DATA)"; \
	if test -f $(top_builddir)/perl/.libs/libConfig.so -o -f $(top_builddir)/perl/libConfig.so; then \
		if test -n "$(PERL)"; then \
			for perlobj in $$MODULES_TO_CHECK; do \
				if test -f $$perlobj; then \
					$(PERL) $(CHECK_PERL_FLAGS) -I$(top_builddir)/perl -I$(top_builddir)/perl/.libs -I$(top_srcdir)/perl -c -T $$perlobj || exit 1; \
				else \
					$(PERL) $(CHECK_PERL_FLAGS) -I$(top_builddir)/perl -I$(top_builddir)/perl/.libs -I$(top_srcdir)/perl -c -T $(top_srcdir)/perl/$$perlobj || exit 1; \
				fi \
			done; \
		fi \
        fi
