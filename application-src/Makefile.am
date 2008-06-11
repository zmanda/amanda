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

SCRIPTS_INCLUDE = \
	    generic-dumper \
	    generic-script

SCRIPTS_PERL = \
	    amstar_perl \
	    amgtar_perl \
	    script-email

SCRIPTS_SHELL = $(amlibexec_SCRIPTS)

EXTRA_DIST += generic-dumper.pl generic-script.pl

CHECK_PERL = $(SCRIPTS_PERL) $(SCRIPTS_INCLUDE)

#We use two variables, applicationdir applicationexecdir, to segregate the
#platform-independent (applicationdir) files installed by install-data from the
#platform-dependent files (applicationexecdir) installed by install-exec. 
#Automake recognizes the 'exec' substring in the variable name.  From the
#Automake manual: "Any variable using a user-defined directory prefix with
#‘exec’ in the name (e.g., myexecbin_PROGRAMS) is installed by install-exec.
#All other user-defined prefixes are installed by install-data." (section 12.2)
applicationexecdir = $(APPLICATION_DIR)
applicationdir = $(APPLICATION_DIR)
applicationexec_PROGRAMS = amgtar amstar
applicationexec_SCRIPTS = $(SCRIPTS_PERL)
application_DATA = $(SCRIPTS_INCLUDE)

LDADD = ../common-src/libamanda.$(LIB_EXTENSION) \
	../client-src/libamclient.$(LIB_EXTENSION) \
	../amandad-src/libamandad.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION) \
	../gnulib/libgnu.$(LIB_EXTENSION)

INSTALLPERMS_exec = dest=$(applicationdir) chown=amanda $(applicationexec_PROGRAMS) \
		    dest=$(applicationdir) chown=amanda $(applicationexec_SCRIPTS)
INSTALLPERMS_data = dest=$(applicationdir) chown=amanda $(application_DATA)
