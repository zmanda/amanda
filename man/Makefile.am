# Makefile for amanda man-pages

.SECONDARY:

transform =	s,x,x,;

AMPLOT_MAN_PAGES =  amplot.8

COMMON_MAN_PAGES =  amanda.8 \
		    amanda.conf.5 \
		    amanda-client.conf.5 \
		    amanda-scripts.7 \
		    amanda-archive-format.5 \
		    amanda-auth.7 \
		    amarchiver.8 \
		    script-email.8

CLIENT_MAN_PAGES = \
    amanda-applications.7 \
    amgtar.8 \
    ampgsql.8 \
    amraw.8 \
    amsamba.8 \
    amstar.8 \
    amsuntar.8 \
    amzfs-snapshot.8 \
    amzfs-sendrecv.8

SERVER_MAN_PAGES =  \
    amanda-devices.7 \
    amanda-compatibility.7 \
    amanda-changers.7 \
    amanda-taperscan.7 \
    amaddclient.8    \
    amadmin.8 \
    amaespipe.8 \
    amcheck.8 \
    amcheckdb.8 \
    amcheckdump.8 \
    amcleanup.8 \
    amcrypt-ossl-asym.8 \
    amcrypt-ossl.8   \
    amcrypt.8 \
    amcryptsimple.8  \
    amdevcheck.8 \
    amdump.8 \
    amflush.8 \
    amgetconf.8 \
    amgpgcrypt.8 \
    amlabel.8 \
    amoverview.8 \
    amreport.8 \
    amrmtape.8 \
    amserverconfig.8 \
    amservice.8 \
    amstatus.8 \
    amtape.8 \
    amtapetype.8 \
    amtoc.8 \
    amvault.8 \
    disklist.5 \
    tapelist.5

RECOVER_MAN_PAGES =  amrecover.8

RESTORE_MAN_PAGES =  amrestore.8 \
		     amfetchdump.8

ALL_MAN_PAGES = $(AMPLOT_MAN_PAGES) \
	   $(CLIENT_MAN_PAGES) \
	   $(COMMON_MAN_PAGES) \
	   $(SERVER_MAN_PAGES) \
	   $(RECOVER_MAN_PAGES) \
	   $(RESTORE_MAN_PAGES)

WANTED_MAN_PAGES = $(COMMON_MAN_PAGES)
if WANT_AMPLOT
  WANTED_MAN_PAGES += $(AMPLOT_MAN_PAGES)
endif
if WANT_CLIENT
  WANTED_MAN_PAGES += $(CLIENT_MAN_PAGES)
endif
if WANT_SERVER
  WANTED_MAN_PAGES += $(SERVER_MAN_PAGES)
endif
if WANT_RECOVER
  WANTED_MAN_PAGES += $(RECOVER_MAN_PAGES)
endif
if WANT_RESTORE
  WANTED_MAN_PAGES += $(RESTORE_MAN_PAGES)
endif

SRCMANPAGEDIR  = $(srcdir)/xml-source

MAN_XML = $(ALL_MAN_PAGES:%=xml-source/%.xml)

EXTRA_XML = xslt/expand-sambadoc.xsl \
            xslt/man.xsl.in \
            xslt/html.xsl.in \
            xslt/settings.xsl \
	    entities/global.entities.in \
	    entities/xinclude.dtd

EXTRA_DIST = $(ALL_MAN_PAGES) $(MAN_XML) $(EXTRA_HTML) $(EXTRA_XML)

GEN_XML = $(ALL_MAN_PAGES:%=xml-source/%.proc.xml)

MOSTLYCLEANFILES = $(GEN_XML)
MAINTAINERCLEANFILES = $(ALL_MAN_PAGES)

# some of the XSL needs to know what docbook-xsl version we've selected
%.xsl: %.xsl.in $(top_builddir)/config.status
	$(top_builddir)/config.status --file=$@:$<
	rm -f config.log

entities/global.entities: entities/global.entities.in $(top_builddir)/config.status
	$(top_builddir)/config.status --file=$@:$<
	rm -f config.log

if   ENABLE_MANPAGE_BUILD
# phase one of the documentation build expands some macros.
xml-source/%.proc.xml: $(SRCMANPAGEDIR)/%.xml $(srcdir)/xslt/expand-sambadoc.xsl entities/global.entities
	$(XSLTPROC) $(XSLTPROC_FLAGS) --path "$(srcdir)/xslt/:$(builddir)/entities/" --xinclude --stringparam latex.imagebasedir "$*/" --stringparam noreference 1 --output $@ $(srcdir)/xslt/expand-sambadoc.xsl $<

# phase two generates a manpage from the resulting "expanded" docbook XML
%: xml-source/%.proc.xml $(srcdir)/xslt/man.xsl entities/global.entities
	$(XSLTPROC) $(XSLTPROC_FLAGS) --path "$(srcdir)/xslt/:$(builddir)/entities/" --output $@ xslt/man.xsl $<

%.html: xml-source/%.proc.xml $(srcdir)/xslt/html.xsl entities/global.entities
	$(XSLTPROC) $(XSLTPROC_FLAGS) --path "$(srcdir)/xslt/:$(builddir)/entities/" --output $@ xslt/html.xsl $<

else	!ENABLE_MANPAGE_BUILD

%: $(SRCMANPAGEDIR)/%.xml
	@echo WARNING: Not building needed $@ because building manpages is disabled.
	@echo 'DUMMY' > $@

# (no rule to build HTML without ENABLE_MANPAGE_BUILD)

endif

##
## HTML Generation
##

EXTRA_DIST += \
	index.php \
	amanda.css

ALL_MAN_HTMLS = $(ALL_MAN_PAGES:%=%.html)

# toplevel rule to build HTML manpages and tar them up
html: amanda.css index.php $(ALL_MAN_HTMLS)
	tar -zcf html.tar.gz $(ALL_MAN_HTMLS) amanda.css index.php

# double-check that we don't ship dummy manpages
dist-hook:
	for f in $(WANTED_MAN_PAGES); do \
		if sed -e 1q $(distdir)/$$f | $(GREP) '^DUMMY$$' >/dev/null 2>/dev/null; then \
			echo "$(distdir)/$$f is a dummy manpage; cannot create distribution"; \
			exit 1; \
		fi; \
	done

man_MANS = $(WANTED_MAN_PAGES)
