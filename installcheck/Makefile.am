# Note that this architecture assumes a full install -- not just server
# or just client.  Neither is sufficiently test-able on its own.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

# Add your tests here.

common_tests = \
	Amanda_Config \
	Amanda_Types

server_tests = \
	Amanda_Changer \
	Amanda_Cmdline \
	Amanda_Logfile \
	amcheckdump \
	amdevcheck \
	amgetconf

tests = $(common_tests)
if WANT_SERVER
tests += $(server_tests)
endif

# Add any common files (that should not be run as tests) here:
test_utils = Amconfig.pm
EXTRA_DIST += $(test_utils)

SCRIPTS_PERL = $(common_tests) $(server_tests) $(test_utils)

# we don't need to syntax check the test scripts..
CHECK_PERL =

.PHONY: clobber_my_config_is_ok
clobber_my_config_is_ok:
	@if test "$(CLOBBER_MY_CONFIG)" != "OK"; then \
		echo ""; \
		echo "'make installcheck' is a dangerous tool.  It will overwrite your"; \
		echo "amanda-client.conf and amandates, and (if it"; \
		echo "triggers an as-yet undetected bug) may do other unexpected things.  You are"; \
		echo "strongly encouraged"; \
 		echo "  - not to run installchecks on a production install"; \
 		echo "  - not to run installchecks as root"; \
		echo "See http://wiki.zmanda.com/index.php/Testing for instructions on setting up a"; \
		echo "test environment in which"; \
		echo "potential damage is limited by your filesystem's permissions.  To actually run"; \
		echo "the installchecks, invoke make"; \
		echo "as follows:"; \
  		echo "  $(MAKE) CLOBBER_MY_CONFIG=OK installcheck"; \
		exit 1; \
	fi

installcheck-local: clobber_my_config_is_ok $(SCRIPTS_PERL)
	$(mkdir_p) $(AMANDA_TMPDIR)
	$(PERL) -I$(srcdir) -I$(builddir) -e 'use Test::Harness qw(&runtests); runtests(@ARGV);' $(tests)
	rm -rf "$(CONFIG_DIR)/TESTCONF"
	rm -rf "$(CONFIG_DIR)/amanda-client.conf"
	rm -rf "$(DEFAULT_AMANDATES_FILE)"
	rm -rf "$(GNUTAR_LISTED_INCREMENTAL_DIR)"
