## Process this file with automake to produce Makefile.in
AUTOMAKE_OPTIONS = 1.4 foreign

ACINCLUDE_M4 = $(srcdir)/acinclude.m4
ACINCLUDE_M4I = $(srcdir)/config/acinclude.m4i
LIBTOOL_M4I = $(srcdir)/config/libtool.m4i
ACINCLUDE_M4_DEPS = $(ACINCLUDE_M4I) $(LIBTOOL_M4I)

if WANT_CLIENT
CLIENT_SUBDIRS = client-src dumper-src
endif
if WANT_TAPE
TAPE_SUBDIRS = tape-src
endif
if WANT_SERVER
SERVER_SUBDIRS = server-src changer-src
endif
if WANT_RESTORE
RESTORE_SUBDIRS = restore-src
endif
if WANT_RECOVER
RECOVER_SUBDIRS = recover-src oldrecover-src
endif
if WANT_AMPLOT
PLOT_SUBDIRS = amplot
endif
# order is significant, don't change it arbitrarily
SUBDIRS = \
	config \
	common-src \
	amandad-src \
	$(TAPE_SUBDIRS) \
	$(CLIENT_SUBDIRS) \
	$(SERVER_SUBDIRS) \
	$(RESTORE_SUBDIRS) \
	$(RECOVER_SUBDIRS) \
	$(PLOT_SUBDIRS) \
	man docs example

pkgdata_DATA = \
	ReleaseNotes			\
	COPYRIGHT

EXTRA_DIST = $(SNAPSHOT_STAMP) \
	$(pkgdata_DATA)			\
        autogen 			\
	contrib/README			\
	contrib/dbbackup.README		\
	contrib/dbbackup.ksh		\
	contrib/dbbackup.sql		\
	contrib/dbbackup.tcl		\
	contrib/mkamandisk		\
	contrib/set_prod_link.pl	\
	contrib/gsc/README		\
	contrib/gsc/cfggsc.c		\
	contrib/gsc/defgsc.c		\
	contrib/gsc/gsc.add		\
	contrib/gsc/gscdd.c		\
	contrib/gsc/gscdds.h		\
	contrib/gsc/makefile		\
	contrib/gsc/tstinq.c		\
	contrib/gsc/ucfggsc.c		\
	patches/regex-3.6alpha.patch	\
	patches/samba-largefs.patch	\
	patches/tar-1.12.patch

$(ACINCLUDE_M4): $(ACINCLUDE_M4_DEPS)
	@-rm -f $@
	@cat $(ACINCLUDE_M4_DEPS) > $@

libtool: $(LIBTOOL_DEPS)
	$(SHELL) ./config.status --recheck

## This is only meaningful for snapshots, but it won't hurt releases.
CONFIG_STATUS = config.status
$(CONFIG_STATUS): $(SNAPSHOT_STAMP)
SNAPSHOT:
	: SNAPSHOT file was removed, will reconfigure...

lint:
	(cd amandad-src; make lint)
	(cd changer-src; make lint)
	(cd client-src; make lint)
	(cd common-src; make lint)
	(cd oldrecover-src; make lint)
	(cd recover-src; make lint)
	(cd restore-src; make lint)
	(cd server-src; make lint)
	(cd tape-src; make lint)

## Do not release the *.test.c sources.  They get built on the fly and
## would contain a path from the distribution machine, which will just
## confuse the target user.

dist-hook:
	find $(distdir)/. -name '*.test.c' -exec rm {} \;
