# Makefile for Amanda tape library.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src \
		-I$(top_srcdir)/gnulib

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

sbin_PROGRAMS =

CHECK_PERL = amtapetype
SCRIPTS_PERL = $(CHECK_PERL)
sbin_SCRIPTS = $(SCRIPTS_PERL)

## libamdevice.la

amlib_LTLIBRARIES = 	libamdevice.la
libamdevice_la_LDFLAGS = -release $(VERSION) 
libamdevice_la_SOURCES = property.c device.c device-queueing.c \
	null-device.c rait-device.c vfs-device.c 
libamdevice_la_LIBADD = ../common-src/libamanda.la

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
	property.h \
	s3.h \
	s3-util.h \
	tape-device.h \
	tape-ops.h
	
aminclude_HEADERS = 
