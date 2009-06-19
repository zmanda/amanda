# Note that this architecture assumes a full install -- not just server
# or just client.  Neither is sufficiently test-able on its own.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

# Add your tests here.

common_tests = \
	Amanda_Archive \
	Amanda_Config \
	Amanda_Debug \
	Amanda_IPC_LineProtocol \
	Amanda_MainLoop \
	Amanda_Header \
	Amanda_Util \
	Amanda_Xfer \
	amarchiver

client_tests = \
	amgtar \
	ampgsql

server_tests = \
	Amanda_Changer \
	Amanda_Changer_compat \
	Amanda_Changer_disk \
	Amanda_Changer_null \
	Amanda_Changer_rait \
	Amanda_Changer_single \
	Amanda_Cmdline \
	Amanda_Disklist \
	Amanda_DB_Catalog \
	Amanda_Device \
	Amanda_Holding \
	Amanda_Logfile \
	Amanda_Process \
	Amanda_Tapelist \
	Amanda_Xfer_serveronly \
	bigint \
	taper \
	amgetconf \
        amtape \
	amtapetype

# (note that _setupcache should come first here)
full_tests = \
	_setupcache \
	amdump \
	amdevcheck \
	amcheckdump \
	amrmtape \
	amservice \
	amstatus \
	amreport \
	pp-scripts

restore_tests = \
	amfetchdump

mock_tests = \
	mock_mtx

# mock scripts
mocks = \
	mock/mtx

tests =

# tests requiring a client and a server (any tests that run amdump)
# these come first to catch any major problems, if possible
if WANT_SERVER
if WANT_CLIENT
tests += $(full_tests)
endif
endif

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
if WANT_RESTORE
tests += $(restore_tests)
endif
endif
endif

# Add any common files (that should not be run as tests, and that do not
# require configure-variable substitution) here:
test_utils = \
	Installcheck.pm \
	Installcheck/Config.pm \
	Installcheck/Mock.pm \
	Installcheck/Run.pm \
	Installcheck/Dumpcache.pm \
	Installcheck/Application.pm \
	Installcheck/Changer.pm
EXTRA_DIST += $(test_utils)

CHECK_PERL_FLAGS=-I$(top_srcdir)/installcheck

SCRIPTS_PERL = $(tests) $(mocks)

# we don't need to syntax check the test scripts..
CHECK_PERL =

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

installcheck-local: clobber_my_config_is_ok $(SCRIPTS_PERL)
	rm -rf "$(AMANDA_TMPDIR)/installchecks"
	$(mkdir_p) "$(AMANDA_TMPDIR)/installchecks"
	@if test -f "$(CONFIG_DIR)/amanda-client.conf" -a ! -f "$(CONFIG_DIR)/amanda-client.conf.install-backup"; then \
	    cp "$(CONFIG_DIR)/amanda-client.conf" "$(CONFIG_DIR)/amanda-client.conf.install-backup"; \
	fi 
	$(PERL) -I$(srcdir) -I. -e 'use Test::Harness qw(&runtests); runtests(@ARGV);' $(tests)
	rm -rf "$(CONFIG_DIR)/TESTCONF"
	@if test -f "$(CONFIG_DIR)/amanda-client.conf.install-backup"; then \
	    mv -f "$(CONFIG_DIR)/amanda-client.conf.install-backup" "$(CONFIG_DIR)/amanda-client.conf"; \
	fi
	rm -rf "$(AMANDA_TMPDIR)/installchecks"
