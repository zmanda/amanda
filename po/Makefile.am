# Makefile for PO directory in any package using GNU gettext.

# Usually the message domain is the same as the package name.
PACKAGE = amanda
MAINTAINER_EMAIL = support@zmanda.com

POFILES = $(wildcard *.po)
#MOFILES = $(patsubst %.po,%.mo,$(POFILES)) 
MOFILES = 

CFILES = $(wildcard $(top_srcdir)/*/*.[ch])
PLFILES = $(wildcard $(top_srcdir)/*/*.pl.in)
SHFILES = $(wildcard $(top_srcdir)/*/*.sh.in)
ALLFILES = $(CFILES) $(PLFILES) $(SHFILES)

# This variable depends on the location of this directory.
top_builddir = ..
localedir=$(datadir)/locale

# These options get passed to xgettext.
XGETTEXT_OPTIONS = --keyword=_ --keyword=_T

# This is the list of locale categories, beyond LC_MESSAGES, for which the
# message catalogs shall be used.  It is usually empty.
EXTRA_LOCALE_CATEGORIES =


GMSGFMT = gmsgfmt
MSGFMT = msgfmt
MSGINIT = msginit
MSGMERGE = msgmerge
MSGMERGE_UPDATE = msgmerge --update
XGETTEXT = xgettext

DISTFILES =			\
	${POFILES}		\
	${MOFILES}		\
	boldquot.sed		\
	en@boldquot.header	\
	en@quot.header		\
	insert-header.sin	\
	Makefile.am		\
	Makefile.in		\
	Makevars		\
	quot.sed		\
	remove-potcdate.sin	\
	Rules-quot		\
	stamp-po

SUFFIXES = .po .mo .sed .sin

.sin.sed:
	sed -e '/^#/d' $< > t-$@
	mv t-$@ $@

all: $(MOFILES)

# This target rebuilds amanda.pot; it is an expensive operation.
# Note that amanda.pot is not touched if it doesn't need to be changed.
amanda.pot: $(ALLFILES)
	@echo $(XGETTEXT) --language=C --output=amanda.pot ... 
	@$(XGETTEXT) --output=amanda.pot --default-domain=amanda           \
	  --add-comments=TRANSLATORS: $(XGETTEXT_OPTIONS) --language=C	   \
	  $(CFILES)
	@echo $(XGETTEXT) --language=Perl --join-existing --output=amanda.pot ... 
	@$(XGETTEXT) --output=amanda.pot --default-domain=amanda           \
	  --add-comments=TRANSLATORS: $(XGETTEXT_OPTIONS) --language=Perl  \
	  --join-existing $(PLFILES)
	@echo $(XGETTEXT) --language=Shell --join-existing --output=amanda.pot ... 
	@$(XGETTEXT) --output=amanda.pot --default-domain=amanda           \
	  --add-comments=TRANSLATORS: $(XGETTEXT_OPTIONS) --language=Shell \
	  --join-existing $(SHFILES)
	@test ! -f amanda.po || {                                          \
	  if test -f $(srcdir)/amanda.pot; then                            \
	    sed -f remove-potcdate.sed < $(srcdir)/amanda.pot > amanda.1po && \
	    sed -f remove-potcdate.sed < amanda.po > amanda.2po &&         \
	    if cmp amanda.1po amanda.2po >/dev/null 2>&1; then             \
	      rm -f amanda.1po amanda.2po amanda.po;                       \
	    else                                                           \
	      rm -f amanda.1po amanda.2po $(srcdir)/amanda.pot &&          \
	      mv amanda.po $(srcdir)/amanda.pot;                           \
	    fi;                                                            \
	  else                                                             \
	    mv amanda.po $(srcdir)/amanda.pot;                             \
	  fi;                                                              \
	}


# This target rebuilds a PO file if amanda.pot has changed.
# Note that a PO file is not touched if it doesn't need to be changed.
%.po: amanda.pot
	@lang=`echo $@ | sed -e 's,.*/,,' -e 's/\.po$$//'`; \
	if test "$$lang" = "en_ZM"; then  \
	  echo msgen -o en.po amanda.pot; \
	  msgen -o en.po amanda.pot; \
	elif test -f "$(srcdir)/$${lang}.po"; then \
	  echo "$${cdcmd}$(MSGMERGE_UPDATE) $${lang}.po amanda.pot"; \
	  cd $(srcdir) && $(MSGMERGE_UPDATE) $${lang}.po amanda.pot; \
	else \
	  echo $(MSGINIT) --locale=$${lang} --input=amanda.pot --output-file=$${lang}.po; \
	  $(MSGINIT) --no-translator --locale=$${lang} --input=amanda.pot --output-file=$${lang}.po; \
	fi

#en_ZM.po: en.po zlocale.pl ztranslation
#	@chmod +x ./zlocale.pl
#	./zlocale.pl			

%.mo: %.po
	@echo "$(MSGFMT) -o $@ $<"; \
	$(MSGFMT) -o t-$@ $< && mv t-$@ $@

install-data-hook: $(MOFILES)
	@for cat in "" $(MOFILES); do \
	  if test -z "$$cat" -o "$$cat" = "en.mo"; then \
	    continue; \
	  fi; \
	  cat=`basename $$cat`; \
	  lang=`echo $$cat | sed -e 's/\.mo$$//'`; \
	  dir=$(localedir)/$$lang/LC_MESSAGES; \
	  $(mkdir_p) $(DESTDIR)$$dir; \
	  if test -r $$cat; then realcat=$$cat; else realcat=$(srcdir)/$$cat; fi; \
	  $(INSTALL) $$realcat $(DESTDIR)$$dir/amanda.mo; \
	  echo "installing $$realcat as $(DESTDIR)$$dir/amanda.mo"; \
	  for lc in '' $(EXTRA_LOCALE_CATEGORIES); do \
	    if test -n "$$lc"; then \
	      if (cd $(DESTDIR)$(localedir)/$$lang && LC_ALL=C ls -l -d $$lc 2>/dev/null) | grep ' -> ' >/dev/null; then \
	        link=`cd $(DESTDIR)$(localedir)/$$lang && LC_ALL=C ls -l -d $$lc | sed -e 's/^.* -> //'`; \
	        mv $(DESTDIR)$(localedir)/$$lang/$$lc $(DESTDIR)$(localedir)/$$lang/$$lc.old; \
	        mkdir $(DESTDIR)$(localedir)/$$lang/$$lc; \
	        (cd $(DESTDIR)$(localedir)/$$lang/$$lc.old && \
	         for file in *; do \
	           if test -f $$file; then \
	             ln -s ../$$link/$$file $(DESTDIR)$(localedir)/$$lang/$$lc/$$file; \
	           fi; \
	         done); \
	        rm -f $(DESTDIR)$(localedir)/$$lang/$$lc.old; \
	      else \
	        if test -d $(DESTDIR)$(localedir)/$$lang/$$lc; then \
	          :; \
	        else \
	          rm -f $(DESTDIR)$(localedir)/$$lang/$$lc; \
	          mkdir $(DESTDIR)$(localedir)/$$lang/$$lc; \
	        fi; \
	      fi; \
	      rm -f $(DESTDIR)$(localedir)/$$lang/$$lc/amanda.mo; \
	      ln -s ../LC_MESSAGES/amanda.mo $(DESTDIR)$(localedir)/$$lang/$$lc/amanda.mo 2>/dev/null || \
	      ln $(DESTDIR)$(localedir)/$$lang/LC_MESSAGES/amanda.mo $(DESTDIR)$(localedir)/$$lang/$$lc/amanda.mo 2>/dev/null || \
	      cp -p $(DESTDIR)$(localedir)/$$lang/LC_MESSAGES/amanda.mo $(DESTDIR)$(localedir)/$$lang/$$lc/amanda.mo; \
	      echo "installing $$realcat link as $(DESTDIR)$(localedir)/$$lang/$$lc/amanda.mo"; \
	    fi; \
	  done; \
	 done
