
## Prepare for SWIGging
##

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

amlibexec_SCRIPTS_PERL_NO_CHECK = bin/app
SCRIPTS_PERL_NO_CHECK = bin/app


##
## HTML Generation
##

###
### Packages
###

# PM_FILES should contain the .pm files from which the HTML should
# be geneated.  This is just the concatenation of foo_DATA for all $(foodir)
# into which perl modules are installed.
PM_FILES=


# PACKAGE: Amanda::Rest
if WANT_SERVER
RestServerdir = $(amlibdir)/rest-server
RestServer_DATA = config.yml
RestServerconfigdancerdir = $(RestServerdir)/config-dancer
RestServerconfigdancer_DATA = config-dancer/config.yml

RestServerEnvironmentsdir = $(RestServerdir)/environments
RestServerEnvironments_DATA = environments/development.yml \
			      environments/production.yml
RestServerpublicdir = $(RestServerdir)/public
RestServerpublic_DATA = public/404.html \
			public/500.html \
			public/dispatch.cgi \
			public/dispatch.fcgi \
			public/favicon.ico
RestServerpubliccssdir = $(RestServerdir)/public/css
RestServerpubliccss_DATA = public/css/error.css \
			   public/css/style.css
RestServerpublicimagesdir = $(RestServerdir)/public/images
RestServerpublicimages_DATA = public/images/perldancer-bg.jpg \
			      public/images/perldancer.jpg
RestAmandapublicjavascriptsdir = $(RestServerdir)/public/javascripts
RestAmandapublicjavascripts_DATA = public/javascripts/jquery.js
RestServerviewsdir = $(RestServerdir)/views
RestServerviews_DATA = views/index.tt
RestServerviewslayoutsdir = $(RestServerdir)/views/layout
RestServerviewslayouts_DATA = views/layouts/main.tt
RestServerbindir = $(RestServerdir)/bin
RestServerbin_DATA = bin/app.pl \
		     bin/app-dancer2.pl
RestServerlibAmandadir = $(RestServerdir)/lib/Amanda
RestServerlibAmanda_DATA = lib/Amanda/Rest.pm

endif
EXTRA_DIST += $(RestServer_DATA) \
	      $(RestServerconfigdancer_DATA) \
	      $(RestServerEnvironments_DATA) \
	      $(RestServerpublic_DATA) \
	      $(RestServerpubliccss_DATA) \
	      $(RestServerpublicimages_DATA) \
	      $(RestAmandapublicjavascripts_DATA) \
	      $(RestServerviews_DATA) \
	      $(RestServerviewslayouts_DATA) \
	      bin/app.pl.in \
	      bin/app-dancer2.pl.in \
	      lib/Amanda/Rest.pm.in
DISTCLEANFILES += lib/Amanda/Rest.pm \
		  bin/app.pl \
		  bin/app-dancer2.pl

# only syntax-check the .pm files if SYNTAX_CHECKS are enabled
check-pm:
	@MODULES_TO_CHECK="$(PM_FILES)"; \
	if test -f $(top_builddir)/perl/.libs/libConfig.so -o -f $(top_builddir)/perl/libConfig.so; then \
	    for perlobj in $$MODULES_TO_CHECK; do \
		R=`expr match "$(SCRIPTS_PERL_NO_CHECK)" ".*$$perlobj.*"`; \
		if [ -z "$(SCRIPTS_PERL_NO_CHECK)" -o $$R -eq 0 ]; then \
		    if test -f $$perlobj; then \
			$(PERL) $(CHECK_PERL_FLAGS) -I$(top_builddir)/perl -I$(top_builddir)/perl/.libs -I$(top_srcdir)/perl -c -T $$perlobj || exit 1; \
		    else \
			$(PERL) $(CHECK_PERL_FLAGS) -I$(top_builddir)/perl -I$(top_builddir)/perl/.libs -I$(top_srcdir)/perl -c -T $(top_srcdir)/perl/$$perlobj || exit 1; \
		    fi \
		fi \
	    done; \
        fi
if SYNTAX_CHECKS
check-local: check-pm
endif
