# Makefile for Amanda tape library.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src \
		-I$(top_srcdir)/xfer-src \
		-I$(top_srcdir)/ndmp-src \
		-I$(top_srcdir)/gnulib

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
if GCC_COMPILER
# ndmp-device.c includes files with old-skool prototypes, so:
AM_CFLAGS += -Wno-strict-prototypes
endif
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS) $(AS_NEEDED_FLAGS)

sbin_PROGRAMS =

SCRIPTS_PERL = \
	amdevcheck \
	amtapetype
sbin_SCRIPTS = $(SCRIPTS_PERL)

CLEANFILES = $(SCRIPTS_PERL) config.log

## libamdevice.la

amlib_LTLIBRARIES = 	libamdevice.la
libamdevice_la_LDFLAGS = -release $(VERSION) $(AS_NEEDED_FLAGS)
libamdevice_la_SOURCES = \
	property.c \
	device.c \
	directtcp-connection.c \
	diskflat-device.c \
	null-device.c \
	rait-device.c \
	vfs-device.c \
	xfer-source-device.c \
	xfer-dest-device.c \
	xfer-dest-taper.c \
	xfer-dest-taper-cacher.c \
	xfer-dest-taper-directtcp.c \
	xfer-dest-taper-splitter.c \
	xfer-source-recovery.c
libamdevice_la_LIBADD = \
	../common-src/libamanda.la \
	../xfer-src/libamxfer.la

if WANT_TAPE_DEVICE
libamdevice_la_SOURCES += tape-device.c
endif

if WANT_S3_DEVICE
libamdevice_la_SOURCES += s3-device.c s3.c s3-util.c
endif

if WANT_DVDRW_DEVICE
libamdevice_la_SOURCES += dvdrw-device.c
endif

if WANT_NDMP_DEVICE
libamdevice_la_SOURCES += ndmp-device.c
libamdevice_la_LIBADD += ../ndmp-src/libndmlib.la
endif

## automake-style tests

TESTS =
noinst_PROGRAMS = $(TESTS)

## activate-devpay

if WANT_S3_DEVICE
sbin_PROGRAMS += activate-devpay
activate_devpay_LDADD = \
	../gnulib/libgnu.la
activate_devpay_SOURCES = activate-devpay.c
endif

## headers

noinst_HEADERS = \
	device.h \
	directtcp-connection.h \
	diskflat-device.h \
	property.h \
	s3.h \
	s3-util.h \
	xfer-device.h \
	xfer-dest-taper.h \
	vfs-device.h

aminclude_HEADERS = 

install-build-dependencies: install-data-am
	sed -e 's@ ${amlibdir}@ ${DESTDIR}${amlibdir}@g' -i ${DESTDIR}/${amlibdir}/libamdevice.la
