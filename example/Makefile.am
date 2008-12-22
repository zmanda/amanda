# Makefile for sample configuration files

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am

amanda_dir    = $(amdatadir)
templatedir   = $(amanda_dir)/template.d
exampledir    = $(amanda_dir)/example
lbltempldir   = $(amanda_dir)/example/label-templates

EXTRA_DIST +=	chg-multi.conf \
		chg-scsi.conf \
		config.site \
		disklist \
		chg-mcutil.conf

dist_template_DATA = template.d/advanced.conf \
		template.d/amanda-harddisk.conf \
		template.d/amanda-single-tape.conf \
		template.d/amanda-tape-changer.conf \
		template.d/amanda-S3.conf \
		template.d/dumptypes \
		template.d/tapetypes \
		template.d/README   \
		template.d/chg-manual.conf   

dist_example_DATA = amanda.conf \
	       amanda-client.conf \
	       inetd.conf.amandaclient \
	       inetd.conf.amandaserver \
	       xinetd.amandaclient \
	       xinetd.amandaserver 

dist_lbltempl_DATA = DLT.ps \
                EXB-8500.ps \
                HP-DAT.ps \
                8.5x11.ps \
                3hole.ps \
		DLT-A4.ps \
                DIN-A4.ps 

INSTALLPERMS_data = chown=amanda \
	dest=$(templatedir) $(dist_template_DATA) \
	dest=$(exampledir) $(dist_example_DATA) \
	dest=$(libtempldir) $(dist_libtempl_DATA) 
