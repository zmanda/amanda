# Makefile for amanda man-pages

transform =	s,x,x,;

if WANT_AMPLOT
AMPLOT_MAN8_PAGES = amplot.8
endif

COMMON_MAN8_PAGES = amanda.8

COMMON_MAN5_PAGES = amanda.conf.5 \
		    amanda-client.conf.5

#if WANT_SERVER
SERVER_MAN8_PAGES = amadmin.8 \
		    amcheck.8 \
		    amcheckdb.8 \
		    amcleanup.8 \
		    amdd.8 \
		    amdump.8 \
		    amflush.8 \
		    amgetconf.8 \
		    amlabel.8 \
		    ammt.8 \
		    amoverview.8 \
		    amreport.8 \
		    amrmtape.8 \
		    amstatus.8 \
		    amtape.8 \
		    amtapetype.8 \
		    amtoc.8 \
		    amverify.8 \
		    amverifyrun.8 \
		    amfetchdump.8 \
		    amcrypt.8 \
		    amaespipe.8 \
		    amcrypt-ossl.8 \
		    amcrypt-ossl-asym.8
#endif

if WANT_RECOVER
RECOVER_MAN8_PAGES = amrecover.8
endif

if WANT_RESTORE
RESTORE_MAN8_PAGES = amrestore.8
endif

man8_pages = $(COMMON_MAN8_PAGES) \
	    $(AMPLOT_MAN8_PAGES) \
	    $(SERVER_MAN8_PAGES) \
	    $(RECOVER_MAN8_PAGES) \
	    $(RESTORE_MAN8_PAGES)

man5_pages = $(COMMON_MAN5_PAGES)

ALL_MAN_PAGES = $(AMPLOT_MAN8_PAGES) \
	   $(COMMON_MAN5_PAGES) \
	   $(COMMON_MAN8_PAGES) \
	   $(SERVER_MAN8_PAGES) \
	   $(RECOVER_MAN8_PAGES) \
	   $(RESTORE_MAN8_PAGES)

# These lines are needed to force automake to set the $man<n>dir variables.
man5_MANS =
man8_MANS =

SRCMANPAGEDIR  = $(srcdir)/xml-source

MAN_XML = $(ALL_MAN_PAGES:%=xml-source/%.xml)

EXTRA_XML = xslt/expand-sambadoc.xsl \
            xslt/man.xsl \
            xslt/settings.xsl \
	    entities/global.entities \
	    entities/xinclude.dtd

EXTRA_DIST = $(ALL_MAN_PAGES) $(MAN_XML) $(EXTRA_XML)

GEN_XML = $(ALL_MAN_PAGES:%=xml-source/%.proc.xml)

MOSTLYCLEANFILES = $(GEN_XML)
MAINTAINERCLEANFILES = $(ALL_MAN_PAGES)

if   ENABLE_MANPAGE_BUILD
xml-source/%.proc.xml: $(SRCMANPAGEDIR)/%.xml $(srcdir)/xslt/expand-sambadoc.xsl
	$(XSLTPROC) $(XSLTPROC_FLAGS) --path $(srcdir)/xslt/ --xinclude --stringparam latex.imagebasedir "$*/" --stringparam noreference 1 --output $@ $(srcdir)/xslt/expand-sambadoc.xsl $<

%: xml-source/%.proc.xml $(srcdir)/xslt/man.xsl
	$(XSLTPROC) $(XSLTPROC_FLAGS) --path $(srcdir)/xslt/ --output $@ http://docbook.sourceforge.net/release/xsl/1.72.0/manpages/docbook.xsl $<

else	!ENABLE_MANPAGE_BUILD

%: $(SRCMANPAGEDIR)/%.xml
	@echo WARNING: Not building needed $@ because building manpages is disabled.
endif

all: $(ALL_MAN_PAGES)

# this is conditioned on ENABLE_MANPAGE_BUILD; it checks for the
# existence of the file to be installed when building manpages is
# not enabled.
install-data-hook: $(ALL_MAN_PAGES)
if	ENABLE_MANPAGE_BUILD
	@list="$(man8_pages)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(man8dir)/`echo $$p|sed '$(transform)'`; \
		echo $(INSTALL) -c -m 644 $$p $(DESTDIR)$(man8dir); \
		$(INSTALL) -c -m 644 $$p $(DESTDIR)$(man8dir); \
		echo chown \"$(BINARY_OWNER)\" $$pa;	\
		chown "$(BINARY_OWNER)" $$pa;		\
		echo chgrp \"$(SETUID_GROUP)\" $$pa;	\
		chgrp "$(SETUID_GROUP)" $$pa;		\
	done
	@list="$(man5_pages)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(man5dir)/`echo $$p|sed '$(transform)'`; \
		echo $(INSTALL) -c -m 644 $$p $(DESTDIR)$(man5dir); \
		$(INSTALL) -c -m 644 $$p $(DESTDIR)$(man5dir); \
		echo chown \"$(BINARY_OWNER)\" $$pa;	\
		chown "$(BINARY_OWNER)" $$pa;		\
		echo chgrp \"$(SETUID_GROUP)\" $$pa;	\
		chgrp "$(SETUID_GROUP)" $$pa;		\
	done
else	!ENABLE_MANPAGE_BUILD
	@list="$(man8_pages)"; \
	for p in $$list; do \
	    if test -e $$p; then \
		pa=$(DESTDIR)$(man8dir)/`echo $$p|sed '$(transform)'`; \
		echo $(INSTALL) -c -m 644 $$p $(DESTDIR)$(man8dir); \
		$(INSTALL) -c -m 644 $$p $(DESTDIR)$(man8dir); \
		echo chown \"$(BINARY_OWNER)\" $$pa;	\
		chown "$(BINARY_OWNER)" $$pa;		\
		echo chgrp \"$(SETUID_GROUP)\" $$pa;	\
		chgrp "$(SETUID_GROUP)" $$pa;		\
	    fi \
	done
	@list="$(man5_pages)"; \
	for p in $$list; do \
	    if test -e $$p; then \
		pa=$(DESTDIR)$(man5dir)/`echo $$p|sed '$(transform)'`; \
		echo $(INSTALL) -c -m 644 $$p $(DESTDIR)$(man5dir); \
		$(INSTALL) -c -m 644 $$p $(DESTDIR)$(man5dir); \
		echo chown \"$(BINARY_OWNER)\" $$pa;	\
		chown "$(BINARY_OWNER)" $$pa;		\
		echo chgrp \"$(SETUID_GROUP)\" $$pa;	\
		chgrp "$(SETUID_GROUP)" $$pa;		\
	    fi \
	done
endif
