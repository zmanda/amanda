# Makefile for sample configuration files

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am

amanda_dir    = $(amdatadir)
templatedir   = $(amanda_dir)/template.d
server_exampledir    = $(amanda_dir)/example
client_exampledir    = $(amanda_dir)/example
lbltempldir   = $(amanda_dir)/example/label-templates

template_in_FILE = \
		template.d/advanced.conf.in \
		template.d/amanda-harddisk.conf.in \
		template.d/amanda-single-tape.conf.in \
		template.d/amanda-tape-changer.conf.in \
		template.d/amanda-S3.conf.in
template_FILE = \
		template.d/dumptypes \
		template.d/tapetypes \
		template.d/README   \
		template.d/chg-manual.conf   
template_generated_FILE = $(template_in_FILE:%.in=%)
template_DATA = $(template_FILE) $(template_generated_FILE)
EXTRA_DIST += $(template_FILE) $(template_in_FILE)
DISTCLEANFILES += $(template_generated_FILE)


server_example_in_FILE = \
		amanda.conf.in \
		inetd.conf.amandaserver.in \
		xinetd.amandaserver.in \
		amandaserver-tcp.xml.in
server_example_FILE = \
		chg-multi.conf \
		chg-scsi.conf \
		disklist
server_example_generated_FILE = $(server_example_in_FILE:%.in=%)
server_example_DATA = $(server_example_FILE) $(server_example_generated_FILE)
EXTRA_DIST += $(server_example_FILE) $(server_example_in_FILE)
DISTCLEANFILES += $(server_example_generated_FILE)

client_example_in_FILE = \
		amanda-client.conf.in \
		amanda-client-postgresql.conf.in \
		inetd.conf.amandaclient.in \
		xinetd.amandaclient.in \
		amandaclient-tcp.xml.in
client_example_FILE = 
client_example_generated_FILE = $(client_example_in_FILE:%.in=%)
client_example_DATA = $(client_example_FILE) $(client_example_generated_FILE)
EXTRA_DIST += $(client_example_FILE) $(client_example_in_FILE)
DISTCLEANFILES += $(client_example_generated_FILE)


lbltempl_DATA = DLT.ps \
                EXB-8500.ps \
                HP-DAT.ps \
                8.5x11.ps \
                3hole.ps \
		DLT-A4.ps \
                DIN-A4.ps 
EXTRA_DIST += $(lbltempl_DATA)

# build foo from foo.in where necessary
%: %.in $(top_builddir)/config.status
	$(top_builddir)/config.status --file=$@:$<

# config.status leaves config.log files around
CLEANFILES += config.log
