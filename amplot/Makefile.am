# Makefile for amplot.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

SCRIPTS_SHELL = amplot
SCRIPTS_AWK = amcat.awk

sbin_SCRIPTS =		$(SCRIPTS_SHELL)
amplotdir =		$(libexecdir)
amplot_DATA =		$(SCRIPTS_AWK) amplot.awk amplot.g amplot.gp

EXTRA_DIST +=		amplot.awk		amplot.g

CLEANFILES += amplot.gp.new
DISTCLEANFILES += amplot.gp

amplot.gp: amplot.g
	cat $(srcdir)/amplot.g > amplot.gp.new
	echo "pause -1;" >> amplot.gp.new
	rm -f amplot.gp
	mv amplot.gp.new amplot.gp

install-exec-hook:
	@list="$(sbin_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(sbindir)/`echo $$p|sed '$(transform)'`; \
		echo chown \"$(BINARY_OWNER)\" $$pa;	\
		chown "$(BINARY_OWNER)" $$pa;		\
		echo chgrp \"$(SETUID_GROUP)\" $$pa;	\
		chgrp "$(SETUID_GROUP)" $$pa;		\
	done

install-data-hook:
	@list="$(amplot_DATA)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(libexecdir)/$$p; \
		echo chown \"$(BINARY_OWNER)\" $$pa;	\
		chown "$(BINARY_OWNER)" $$pa;		\
		echo chgrp \"$(SETUID_GROUP)\" $$pa;	\
		chgrp "$(SETUID_GROUP)" $$pa;		\
        done
