# Makefile for Amanda archive library

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =      -I$(top_builddir)/common-src \
                -I$(top_srcdir)/common-src \
                -I$(top_srcdir)/gnulib

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

amlib_LTLIBRARIES =     libamar.la

libamar_la_SOURCES = amar.c
libamar_la_LDFLAGS =  -release $(VERSION)
libamar_la_LIBADD = ../common-src/libamanda.la

noinst_HEADERS = \
	amar.h

sbin_PROGRAMS = amarchiver

amarchiver_SOURCES = amarchiver.c
amarchiver_LDADD = libamar.la

# automake-style tests

TESTS = amar-test
noinst_PROGRAMS = $(TESTS)

amar_test_SOURCES = amar-test.c
amar_test_LDADD = libamar.la \
	../common-src/libtestutils.la
