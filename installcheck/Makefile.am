# Note that this architecture assumes a full install -- not just server
# or just client.  Neither is sufficiently test-able on its own.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

# Add your tests here.
tests = \
	Amanda_Logfile \
	Amanda_Changer \
	Amanda_Cmdline \
	Amanda_Config \
	amcheckdump \
	amdevcheck \
	amgetconf

# Add any common files (that should not be run as tests) here:
test_utils = Amconfig.pm

SCRIPTS_PERL = $(tests) $(test_utils)

# we don't need to syntax check the test scripts..
CHECK_PERL =

installcheck-local: $(SCRIPTS_PERL)
	$(PERL) -I. -e 'use Test::Harness qw(&runtests); runtests(@ARGV);' $(tests)
	rm -rf "$(CONFIG_DIR)/TESTCONF"
