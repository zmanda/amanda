# Makefile for sample configuration files

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am

amanda_dir    = $(amdatadir)
templatedir   = $(amanda_dir)/template.d
exampledir    = $(amanda_dir)/example
lbltempldir   = $(amanda_dir)/example/label-templates

template_DATA = template.d/advanced.conf \
		template.d/amanda-harddisk.conf \
		template.d/amanda-single-tape.conf \
		template.d/amanda-tape-changer.conf \
		template.d/amanda-S3.conf \
		template.d/dumptypes \
		template.d/tapetypes \
		template.d/README   \
		template.d/chg-manual.conf   

example_DATA = amanda.conf \
	       amanda-client.conf \
	       amanda-client-postgresql.conf \
	       inetd.conf.amandaclient \
	       inetd.conf.amandaserver \
	       xinetd.amandaclient \
	       xinetd.amandaserver \
	       chg-multi.conf \
	       chg-scsi.conf \
	       disklist

lbltempl_DATA = DLT.ps \
                EXB-8500.ps \
                HP-DAT.ps \
                8.5x11.ps \
                3hole.ps \
		DLT-A4.ps \
                DIN-A4.ps 

# build foo from foo.in where necessary
%: %.in $(top_builddir)/config.status
	$(top_builddir)/config.status --file=$@:$<

# config.status leaves config.log files around
CLEANFILES += config.log

# figure out whether to put foo.in or foo itself into the dist tarball, based on whether
# foo.in exists.
EXAMPLE_FILES = $(template_DATA) $(example_DATA) $(lbltempl_DATA)
DISTCLEANFILES += $(EXAMPLE_FILES)
dist-example-files:
	@EXAMPLE_FILES="$(EXAMPLE_FILES)"; \
	for file in $$EXAMPLE_FILES; do \
		test -f $(srcdir)/$${file}.in && file=$${file}.in; \
		dir=`dirname $${file}`; \
		if test -n "$$dir" && test ! -d "$(distdir)/$$dir"; then \
			mkdir -p "$(distdir)/$$dir" || exit 1; \
		fi; \
		echo "distributing $${file}"; \
		cp -p "$(srcdir)/$${file}" "$(distdir)/$${file}" || exit 1; \
	done
dist-hook: dist-example-files

INSTALLPERMS_data = chown=amanda \
	dest=$(templatedir) $(template_DATA) \
	dest=$(exampledir) $(example_DATA) \
	dest=$(lbltempldir) $(lbltempl_DATA)
