# Makefile for Amanda tape library.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src \
		-I$(top_srcdir)/gnulib \
		-I..

noinst_PROGRAMS = queue_test device_test
LDADD = ../../common-src/libamanda.la \
	../libamdevice.la \
	../../common-src/libamanda.la \
	../../gnulib/libgnu.la

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

