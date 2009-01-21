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

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

LIB_EXTENSION = la

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

CHECK_PERL = amgtar_perl \
	     script-email \
	     amlog-script \
	     amzfs-sendrecv \
	     amzfs-snapshot \
	     amsamba

SCRIPTS_PERL = $(CHECK_PERL)

SCRIPTS_SHELL = $(amlibexec_SCRIPTS)

amlibexec_PROGRAMS = teecount

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

LDADD = ../common-src/libamanda.$(LIB_EXTENSION) \
	../client-src/libamclient.$(LIB_EXTENSION) \
	../amandad-src/libamandad.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION) \
	../gnulib/libgnu.$(LIB_EXTENSION)

INSTALLPERMS_exec = dest=$(amlibexecdir) chown=amanda $(amlibexec_PROGRAMS) \
		    dest=$(applicationdir) chown=amanda $(applicationexec_PROGRAMS) \
		    dest=$(applicationdir) chown=amanda $(applicationexec_SCRIPTS)

INSTALLPERMS_data = dest=$(amlibexecdir) chown=amanda $(amlibexec_SCRIPTS)

if WANT_SETUID_CLIENT
INSTALLPERMS_exec += dest=$(applicationdir) chown=root chmod=04750 \
		     amgtar amstar
endif
