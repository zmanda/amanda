# Makefile for Amanda file recovery programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src   \
		-I$(top_srcdir)/client-src   \
		-I$(top_srcdir)/gnulib

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

sbin_PROGRAMS =		amoldrecover

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.la \
	@LEXLIB@ \
	$(READLINE_LIBS) \
	../client-src/libamclient.la \
	../common-src/libamanda.la

amoldrecover_CSRC =	amrecover.c					\
			display_commands.c		extract_list.c	\
			help.c				set_commands.c

amoldrecover_SOURCES =	$(amoldrecover_CSRC)	uparse.y	uscan.l

noinst_HEADERS = 	amrecover.h uparse.h

AM_YFLAGS =		-d

# so that uscan.c is never generated before uparse.h
# otherwise we might have makedepend problems
uscan.c: uparse.h

# these two commands are specially defined to omit $(AMANDA_WARNING_CFLAGS)
# since we don't want warnings for generated code
uscan.$(OBJEXT): uscan.c
	$(CC) $(CFLAGS) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) $(CPPFLAGS) -c $<

uparse.$(OBJEXT): uparse.c
	$(CC) $(CFLAGS) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) $(CPPFLAGS) -c $<


INSTALLPERMS_exec = \
	dest=$(sbindir) chown=amanda chmod=0750 $(sbin_PROGRAMS)

lint:
	@ f="$(amoldrecover_CSRC)";					  	\
	(cd ../common-src; make listlibsrc);					\
	f="$$f "`cat ../common-src/listlibsrc.output`;				\
	(cd ../server-src; make listlibsrc);					\
	f="$$f "`cat ../server-src/listlibsrc.output`;				\
	echo $(LINT) $$f;							\
	$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I$(top_builddir)/config $(INCLUDES) $$f;\
	if [ $$? -ne 0 ]; then							\
	    exit 1;								\
	fi;									\
        exit 0
