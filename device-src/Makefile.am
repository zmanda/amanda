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
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

sbin_PROGRAMS =

SCRIPTS_PERL = \
	amdevcheck \
	amtapetype
sbin_SCRIPTS = $(SCRIPTS_PERL)

## libamdevice.la

amlib_LTLIBRARIES = 	libamdevice.la
libamdevice_la_LDFLAGS = -release $(VERSION) 
libamdevice_la_SOURCES = \
	property.c \
	device.c \
	device-queueing.c \
	directtcp-connection.c \
	null-device.c \
	rait-device.c \
	vfs-device.c \
	xfer-source-device.c \
	xfer-dest-device.c \
	xfer-dest-taper.c \
	xfer-dest-taper-splitter.c \
	xfer-source-taper.c
libamdevice_la_LIBADD = \
	../common-src/libamanda.la \
	../xfer-src/libamxfer.la

# tape-device has *lots* of conditionals; the first is whether to support
# the device at all, and the remainder select a particular kind of OS-specific
# backend.

if WANT_TAPE_DEVICE
libamdevice_la_SOURCES += tape-device.c

if WANT_TAPE_XENIX
libamdevice_la_SOURCES += tape-xenix.c
endif
if WANT_TAPE_AIX
libamdevice_la_SOURCES += tape-aix.c
endif
if WANT_TAPE_UWARE
libamdevice_la_SOURCES += tape-uware.c
endif
if WANT_TAPE_POSIX
libamdevice_la_SOURCES += tape-posix.c
endif

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

TESTS = vfs-test
noinst_PROGRAMS = $(TESTS)

vfs_test_SOURCES = vfs-test.c
vfs_test_LDADD = \
	../common-src/libtestutils.la \
	libamdevice.la

## activate-devpay

if WANT_S3_DEVICE
sbin_PROGRAMS += activate-devpay
activate_devpay_SOURCES = activate-devpay.c
endif

## headers

noinst_HEADERS = \
	device-queueing.h \
	device.h \
	directtcp-connection.h \
	property.h \
	s3.h \
	s3-util.h \
	tape-device.h \
	tape-ops.h \
	xfer-device.h \
	vfs-device.h

aminclude_HEADERS = 
