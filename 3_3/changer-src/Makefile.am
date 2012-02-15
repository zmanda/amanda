# Makefile for Amanda tape changer programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/installperms.am

SCRIPTS_SHELL = \
		chg-manual \
		chg-multi \
		chg-zd-mtx \
		chg-disk

SCRIPTS_INCLUDE = \
		chg-lib.sh
EXTRA_DIST += chg-lib.sh.in

amlibexec_SCRIPTS = $(SCRIPTS_SHELL)
amlibexec_DATA = $(SCRIPTS_INCLUDE)
