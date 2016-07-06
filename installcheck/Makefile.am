include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
all_tests =

common_tests = \
	Amanda_Archive \
	Amanda_Config \
	Amanda_Config_FoldingHash \
	Amanda_ClientService \
	Amanda_Debug \
	Amanda_Feature \
	Amanda_IPC_Binary \
	Amanda_IPC_LineProtocol \
	Amanda_MainLoop \
	Amanda_Header \
	Amanda_Util \
	Amanda_Xfer \
	amarchiver
all_tests += $(common_tests)

client_tests = \
        noop \
	ambsdtar \
	amgtar \
	ampgsql \
	amraw \
	amstar \
	runtar
all_tests += $(client_tests)

server_tests = \
	Amanda_Changer \
	Amanda_Changer_disk \
	Amanda_Changer_diskflat \
	Amanda_Changer_multi \
	Amanda_Changer_null \
	Amanda_Changer_rait \
	Amanda_Changer_robot \
	Amanda_Changer_single \
	Amanda_Cmdline \
	Amanda_Curinfo \
	Amanda_Disklist \
	Amanda_DB_Catalog \
	Amanda_Device \
	Amanda_Holding \
	Amanda_Logfile \
	Amanda_Process \
	Amanda_Recovery_Clerk \
	Amanda_Recovery_Planner \
	Amanda_Recovery_Scan \
	Amanda_Report \
	Amanda_Rest_Configs \
	Amanda_Rest_Dles \
	Amanda_Rest_Dumps \
	Amanda_Rest_Labels \
	Amanda_Rest_Runs \
	Amanda_Rest_Services_senddiscover \
	Amanda_Rest_Services_senddiscover_amgtar \
	Amanda_Rest_Storages \
	Amanda_Rest_Storages_Labels \
	Amanda_Rest_Version \
	Amanda_Tapelist \
	Amanda_Taper_Scan \
	Amanda_Taper_Scan_lexical \
	Amanda_Taper_Scan_oldest \
	Amanda_Taper_Scan_traditional \
	Amanda_Taper_Scribe \
	bigint \
	taper \
	amcheck-device \
	amgetconf \
        amtape \
        amlabel \
	amtapetype \
	chunker \
	dump-client-custom-compress+amfetchdump \
	dump-server-custom-compress+amfetchdump \
	dump-client-encrypt+amfetchdump \
	dump-server-encrypt+amfetchdump \
	success-dumper-chunker-taper \
	success-dumper-chunker-2taper \
	success-dumper-chunker-2taper-by-level \
	success-dumper-taper \
	success-dumper-chunker+dumper-chunker-taper-flush \
	success-dumper-chunker+taper-flush

all_tests += $(server_tests)

full_tests = \
	0_setupcache \
	Amanda_Rest_Amcheck \
	amadmin \
	amcheck \
	amcheckdump \
	amdevcheck \
	amdump \
	amdump_client \
	amflush \
	amoverview \
	amreport \
	amrmtape \
	amserverconfig \
	amservice \
	amstatus \
	amvault \
	example \
	pp-scripts \
	tape_action
all_tests += $(full_tests)

restore_tests = \
	amfetchdump \
	amidxtaped \
	amrestore
all_tests += $(restore_tests)

restore_tests_no_check_userid = \
	amrecover
all_tests += $(restore_tests_no_check_userid)

ndmp_tests = \
	Amanda_Changer_ndmp \
	Amanda_NDMP
all_tests += $(ndmp_tests)

mock_tests = \
	mock_mtx
all_tests += $(mock_tests)

# mock scripts
mocks = \
	mock/mail \
	mock/mtx \
	mock/lpr

# data for test scripts
test_data = \
	data/aws-bundle.crt
EXTRA_DIST += $(test_data)
CLEANFILES += $(all_tests) $(mocks) $(noinst_SCRIPTS)
DISTCLEANFILES += config.log

tests =

# tests requiring neither client nor server
tests += $(common_tests)
tests += $(mock_tests)

# tests requiring server
if WANT_SERVER
tests += $(server_tests)
endif

# tests requiring client
if WANT_CLIENT
tests += $(client_tests)
endif

# tests requiring a client and a server AND restore facilities (any tests that run amdump)
if WANT_SERVER
if WANT_CLIENT
# tests requiring a client and a server (any tests that run amdump)
tests += $(full_tests)

# tests requiring a client, a server, *and* restore code
if WANT_RESTORE
tests += $(restore_tests)

# and tests requiring all that *and* requiring CHECK_USERID is not set
if !CHECK_USERID
tests += $(restore_tests_no_check_userid)
endif
endif
endif
endif

# tests that need a server with NDMP support
if WANT_NDMP
if WANT_SERVER
tests += $(ndmp_tests)
endif
endif

# Add any common files (that should not be run as tests, and that do not
# require configure-variable substitution) here:
test_utils = \
	Installcheck.pm \
	Installcheck/Catalogs.pm \
	Installcheck/Config.pm \
	Installcheck/ClientService.pm \
	Installcheck/Mock.pm \
	Installcheck/Rest.pm \
	Installcheck/Run.pm \
	Installcheck/Dumpcache.pm \
	Installcheck/Application.pm \
	Installcheck/Changer.pm
EXTRA_DIST += $(test_utils)

# the catalogs themselves
catalogs = \
	catalogs/amflush.cat \
	catalogs/bigdb.cat \
	catalogs/bigestimate.cat \
	catalogs/chunker-partial.cat \
	catalogs/doublefailure.cat \
	catalogs/dumper-tryagain.cat \
	catalogs/fatal.cat \
	catalogs/filesystemstaped.cat \
	catalogs/flush-noorigsize.cat \
	catalogs/flush-origsize.cat \
	catalogs/longstrange.cat \
	catalogs/multi-taper.cat \
	catalogs/normal.cat \
	catalogs/number.cat \
	catalogs/plannerfail.cat \
	catalogs/quoted.cat \
	catalogs/resultsmissing.cat \
	catalogs/retried.cat \
	catalogs/retried-nofinish.cat \
	catalogs/retried-strange.cat \
	catalogs/shortstrange.cat \
	catalogs/skipped.cat \
	catalogs/spanned.cat \
	catalogs/dumper-chunker-taper-success.cat \
	catalogs/dumper-taper-success.cat \
	catalogs/strontium.cat \
	catalogs/taperr.cat \
	catalogs/tapers.cat \
	catalogs/tapers1.cat \
	catalogs/taper-parallel-write.cat
EXTRA_DIST += $(catalogs)


# and finally some development utilities
noinst_SCRIPTS = \
	run-ndmp

CHECK_PERL_FLAGS=-I$(top_srcdir)/installcheck

SCRIPTS_PERL = $(tests) $(mocks) $(noinst_SCRIPTS)
SCRIPTS_EXTRA_DIST = $(all_tests)

# skip syntax checks on these files, since we're about to run all of them
SKIP_CHECKS = yes

.PHONY: clobber_my_config_is_ok
clobber_my_config_is_ok:
	@if test "$(CONFIG_CLOBBER_MY_CONFIG)" != "OK" -a \
                 "$(CLOBBER_MY_CONFIG)" != "OK"; then \
		echo ""; \
		echo "'make installcheck' is a dangerous tool.  It will overwrite your"; \
		echo "amanda-client.conf and amandates, and (if it triggers an as-yet undetected"; \
		echo "bug) may do other unexpected things.  You are strongly encouraged"; \
		echo "  - not to run installchecks on a production install"; \
		echo "  - not to run installchecks as root"; \
		echo "See http://wiki.zmanda.com/index.php/Testing for instructions on setting up a"; \
		echo "test environment in which potential damage is limited by your filesystem's"; \
		echo "permissions.  To actually run the installchecks, invoke make as follows:"; \
		echo "  $(MAKE) CLOBBER_MY_CONFIG=OK installcheck"; \
		exit 1; \
	fi

# newer Test::Harness (v3.18-3.21) implementations get confused by executable
# test scripts, so this rule chmod a-x's them first
installcheck-local: clobber_my_config_is_ok $(SCRIPTS_PERL)
	rm -rf "$(AMANDA_TMPDIR)/installchecks"
	$(MKDIR_P) "$(AMANDA_TMPDIR)/installchecks"
	@if test -f "$(CONFIG_DIR)/amanda-client.conf" -a ! -f "$(CONFIG_DIR)/amanda-client.conf.install-backup"; then \
	    cp "$(CONFIG_DIR)/amanda-client.conf" "$(CONFIG_DIR)/amanda-client.conf.install-backup"; \
	fi
	@if test -f "$(CONFIG_DIR)/template.d/dumptypes" -a ! -f "$(CONFIG_DIR)/template.d/dumptypes.install-backup"; then \
	    cp "$(CONFIG_DIR)/template.d/dumptypes" "$(CONFIG_DIR)/template.d/dumptypes.install-backup"; \
	fi
	@if test -f "$(CONFIG_DIR)/template.d/tapetypes" -a ! -f "$(CONFIG_DIR)/template.d/tapetypes.install-backup"; then \
	    cp "$(CONFIG_DIR)/template.d/tapetypes" "$(CONFIG_DIR)/template.d/tapetypes.install-backup"; \
	fi
	for f in $(tests); do chmod a-x $$f; done
	srcdir="$(srcdir)" $(PERL) -I$(srcdir) -I. -e 'use Test::Harness qw(&runtests); runtests(sort @ARGV);' $(tests)
	rm -rf "$(CONFIG_DIR)/TESTCONF"
	@if test -f "$(CONFIG_DIR)/amanda-client.conf.install-backup"; then \
	    mv -f "$(CONFIG_DIR)/amanda-client.conf.install-backup" "$(CONFIG_DIR)/amanda-client.conf"; \
	else \
	    rm "$(CONFIG_DIR)/amanda-client.conf"; \
	fi
	@if test -f "$(CONFIG_DIR)/template.d/dumptypes.install-backup"; then \
	    mv -f "$(CONFIG_DIR)/template.d/dumptypes.install-backup" "$(CONFIG_DIR)/template.d/dumptypes"; \
	else \
	    rm -f "$(CONFIG_DIR)/template.d/dumptypes"; \
	fi
	@if test -f "$(CONFIG_DIR)/template.d/tapetypes.install-backup"; then \
	    mv -f "$(CONFIG_DIR)/template.d/tapetypes.install-backup" "$(CONFIG_DIR)/template.d/tapetypes"; \
	else \
	    rm -f "$(CONFIG_DIR)/template.d/tapetypes"; \
	fi
	rm -rf "$(AMANDA_TMPDIR)/installchecks"
