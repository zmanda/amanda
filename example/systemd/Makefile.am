# Makefile for sample configuration files

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am

%: %.in $(top_builddir)/config.status
	$(top_builddir)/config.status --file=$@:$<

systemddir = /lib/systemd/system

if WANT_SERVER

systemd_units_in_FILE = \
    amanda-server.socket.in \
    amanda-server@.service.in \
    amanda-rest-server.service.in

# if a net-proxy to the rest server is needed [not currently]
#    amanda-rest-server-pxy.service.in
#    amanda-rest-server-pxy.socket.in
else

# compatible access path for (old) Debian and all Red Hat
systemd_units_in_FILE = \
    amanda-client.socket.in \
    amanda-client@.service.in

endif

systemd_generated_FILE = $(systemd_units_in_FILE:%.in=%)

systemd_DATA = $(systemd_generated_FILE)
EXTRA_DIST += $(systemd_units_in_FILE)
DISTCLEANFILES += $(systemd_generated_FILE)

# install-data-local: $(systemd_DATA)
#	mkdir -p $(DESTDIR)$(systemddir)
#	install -m 644 $(systemd_DATA) $(DESTDIR)$(systemddir)
