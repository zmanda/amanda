# Makefile for amplot.

SUFFIXES =		.sh

.sh:
			cat $< > $@
			chmod a+x $@

sbin_SCRIPTS =		amplot

amplot_DATA =		amcat.awk amplot.awk amplot.g amplot.gp

amplotdir =		$(libexecdir)

EXTRA_DIST =		amplot.awk		amplot.g

CLEANFILES = amplot.gp.new
DISTCLEANFILES = amplot.gp amplot

amplot.gp:		amplot.g
			cat $(srcdir)/amplot.g > amplot.gp.new
			echo "pause -1;" >> amplot.gp.new
			rm -f amplot.gp
			mv amplot.gp.new amplot.gp

install-exec-hook:
	@list="$(sbin_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(sbindir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
	done

install-data-hook:
	@list="$(amplot_DATA)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(libexecdir)/$$p; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
        done
