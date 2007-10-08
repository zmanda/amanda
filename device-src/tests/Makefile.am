# Makefile for Amanda tape library.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src \
		-I$(top_srcdir)/gnulib \
		-I$(top_srcdir)/device-src

# automake-style tests

noinst_PROGRAMS = queue_test device_test $(TESTS)

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###
LDADD = ../../common-src/libamanda.la \
	../libamdevice.la \
	../../common-src/libamanda.la \
	../../gnulib/libgnu.la


TESTS = semaphore-test vfs_test

semaphore_test_SOURCES = semaphore-test.c

vfs_test_SOURCES = vfs_test.c
