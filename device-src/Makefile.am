# Makefile for Amanda tape library.

include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src \
		-I$(top_srcdir)/gnulib

lib_LTLIBRARIES = 	libamdevice.la
LIB_EXTENSION = la

libamdevice_la_SOURCES = property.c device.c queueing.c semaphore.c \
	null-device.c rait-device.c vfs-device.c 

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

noinst_HEADERS = device.h null-device.h queueing.h semaphore.h \
	tape-ops.h property.h rait-device.h \
	tape-device.h vfs-device.h


SUBDIRS = . tests

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

