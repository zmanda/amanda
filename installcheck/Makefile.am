# Note that this architecture assumes a full install -- not just server
# or just client.  Neither is sufficiently test-able on its own.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

# Add your tests here.
tests = amgetconf

# Add any common files (that should not be run as tests) here:
test_utils = Amconfig.pm Installdirs.pm

SCRIPTS_PERL = $(tests) $(test_utils)

# The test modules 'use' test_utils, which are in the current directory. checks
# run with taint mode on, which leaves '.' out of @INC.  So we add it back in
# with this flag.
CHECK_PERL_FLAGS=-I.

installcheck-local: $(SCRIPTS_PERL)
	$(PERL) -I. -e 'use Test::Harness qw(&runtests); use Installdirs; Installdirs::set_top_builddir("$(top_builddir)"); runtests(@ARGV);' $(tests)
	rm -rf "$(CONFIG_DIR)/TESTCONF"
