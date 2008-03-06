# Makefile for Amanda wrapper programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src \
		-I$(top_srcdir)/client-src \
		-I$(top_srcdir)/amandad-src \
		-I$(top_srcdir)/gnulib

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

LIB_EXTENSION = la

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

SCRIPTS_PERL = \
	    amstar_perl \
	    amgtar_perl \
	    script-email

SCRIPTS_INCLUDE = \
	    generic-dumper \
	    generic-script
EXTRA_DIST += generic-dumper.pl generic-script.pl

CHECK_PERL = $(SCRIPTS_PERL) $(SCRIPTS_INCLUDE)

applicationdir = $(APPLICATION_DIR)
application_PROGRAMS = amgtar amstar
application_SCRIPTS = $(SCRIPTS_PERL)
application_DATA = $(SCRIPTS_INCLUDE)

LDADD = ../common-src/libamanda.$(LIB_EXTENSION) \
	../client-src/libamclient.$(LIB_EXTENSION) \
	../amandad-src/libamandad.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION) \
	../gnulib/libgnu.$(LIB_EXTENSION)

INSTALLPERMS_exec = dest=$(applicationdir) chown=amanda $(application_PROGRAMS)
INSTALLPERMS_data = dest=$(applicationdir) chown=amanda $(application_SCRIPTS)\
		    $(application_DATA)
