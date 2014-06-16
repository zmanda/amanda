## Process this file with automake to produce Makefile.in
AUTOMAKE_OPTIONS = 1.10 foreign

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am

ACLOCAL_AMFLAGS = --force -I config -I . -I config/gettext-macros -I config/gnulib -I config/amanda -I config/macro-archive

if WANT_CLIENT
CLIENT_SUBDIRS = client-src application-src
endif
if WANT_SERVER
SERVER_SUBDIRS = device-src server-src
endif
if WANT_RECOVER
RECOVER_SUBDIRS = recover-src oldrecover-src
endif
if WANT_AMPLOT
PLOT_SUBDIRS = amplot
endif
if WANT_NDMP
NDMP_SUBDIRS = ndmp-src
endif
# order is significant, don't change it arbitrarily
SUBDIRS = . \
	gnulib \
	config \
	common-src \
	amar-src \
	amandad-src \
	xfer-src \
	$(NDMP_SUBDIRS) \
	$(TAPE_SUBDIRS) \
	$(CLIENT_SUBDIRS) \
	$(SERVER_SUBDIRS) \
	$(RESTORE_SUBDIRS) \
	$(RECOVER_SUBDIRS) \
	$(PLOT_SUBDIRS) \
	perl \
	po \
	man \
	example \
	packaging \
	installcheck

pkgdata_DATA = \
	ReleaseNotes	\
	COPYRIGHT	\
	NEWS		\
	ChangeLog

EXTRA_DIST += $(SNAPSHOT_STAMP) \
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
	patches/tar-1.12.patch		\
	UPGRADING			\
	DEVELOPING			\
	VERSION				\
	FULL_VERSION

FULL_VERSION: VERSION
	$(srcdir)/config/set_full_version $(top_srcdir)

config.status: FULL_VERSION

libtool: $(LIBTOOL_DEPS)
	$(SHELL) ./config.status --recheck

# empty out the installperms manifest file when we start
install-exec-local: installperms-init
install-data-local: installperms-init

## This is only meaningful for snapshots, but it won't hurt releases.
CONFIG_STATUS = config.status
$(CONFIG_STATUS): $(SNAPSHOT_STAMP)
SNAPSHOT:
	: SNAPSHOT file was removed, will reconfigure...

lint:
	(cd amandad-src; make lint)
	(cd client-src; make lint)
	(cd common-src; make lint)
	(cd oldrecover-src; make lint)
	(cd recover-src; make lint)
	(cd server-src; make lint)
	(cd xfer-src; make lint)

## Do not release the *.test.c sources.  They get built on the fly and
## would contain a path from the distribution machine, which will just
## confuse the target user.

dist-hook:
	find $(distdir)/. -name '*.test.c' -exec rm {} \;

install-build-dependencies:
	rm -rf ${DESTDIR}
	mkdir -p ${DESTDIR}
if WANT_SERVER
	(cd common-src ; make install-build-dependencies)
	(cd xfer-src ; make install-build-dependencies)
	(cd ndmp-src ; make install-build-dependencies)
	(cd device-src ; make install-build-dependencies)
	mkdir ${DESTDIR}/include
	mkdir ${DESTDIR}/include/common-src
	mkdir ${DESTDIR}/include/device-src
	mkdir ${DESTDIR}/include/xfer-src
	mkdir ${DESTDIR}/include/ndmp-src
	mkdir ${DESTDIR}/include/amandad-src
	mkdir ${DESTDIR}/include/server-src
	mkdir ${DESTDIR}/include/client-src
	mkdir ${DESTDIR}/include/recover-src
	mkdir ${DESTDIR}/include/oldrecover-src
	mkdir ${DESTDIR}/include/amar-src
	mkdir ${DESTDIR}/include/contrib
	mkdir ${DESTDIR}/include/contrib/gsc
	mkdir ${DESTDIR}/include/config
	mkdir ${DESTDIR}/include/config/snippet
	mkdir ${DESTDIR}/include/perl
	mkdir ${DESTDIR}/include/perl/amglue
	mkdir ${DESTDIR}/include/gnulib
	mkdir ${DESTDIR}/include/gnulib/arpa
	mkdir ${DESTDIR}/include/gnulib/glthread
	mkdir ${DESTDIR}/include/gnulib/sys
	find . -name '*.h' -exec cp {} ${DESTDIR}/include/{} \;
	mkdir ${DESTDIR}/config
	cp -av config/amanda config/gnulib config/macro-archive ${DESTDIR}/config
endif
	echo "${CONFIGURE_ARGS}" > ${DESTDIR}/configure_args

# ensure that configure gets the right arguments for distcheck; this keeps the
# user/group through to the distcheck, rather than defaulting back to 'amanda'.
DISTCHECK_CONFIGURE_FLAGS = --with-user=$(CLIENT_LOGIN) --with-group=$(SETUID_GROUP) --with-owner=$(BINARY_OWNER) --disable-installperms --without-amperldir --without-force-uid --with-tmpdir=$(AMANDA_TMPDIR) SINGLE_USERID=yes CLOBBER_MY_CONFIG=OK
