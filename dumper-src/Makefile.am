# Makefile for Amanda wrapper programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

SCRIPTS_PERL = \
	    amgtar

SCRIPTSDATA = \
	    generic-dumper

CHECK_PERL = $(SCRIPTS_PERL) $(SCRIPTSDATA)

dumperdir = $(DUMPER_DIR)
dumper_SCRIPTS = $(SCRIPTS_PERL)
dumper_DATA = $(SCRIPTSDATA)

INSTALLPERMS_data = dest=$(dumperdir) chown=amanda $(dumper_SCRIPTS) $(dumper_DATA)
