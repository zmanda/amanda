# Makefile for sample configuration files

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am

%: %.in $(top_builddir)/config.status
	$(top_builddir)/config.status --file=$@:$<

if WANT_SERVER

systemddir = /opt/zmanda/amanda/etc/systemd/system
systemd_units_in_FILE = \
    __sub-amanda-server.socket.in \
    __sub-amanda-server@.service.in \
    __sub-amanda-rest-server.service.in

# if a net-proxy to the rest server is needed [not currently]
#    __sub-amanda-rest-server-pxy.service.in
#    __sub-amanda-rest-server-pxy.socket.in
else

# compatible access path for (old) Debian and all Red Hat
systemddir = /lib/systemd/system
systemd_units_in_FILE = \
    __sub-amanda-client.socket.in \
    __sub-amanda-client@.service.in

endif

systemd_generated_FILE = $(systemd_units_in_FILE:%.in=%)

systemd_DATA = $(systemd_generated_FILE)
EXTRA_DIST += $(systemd_units_in_FILE)
DISTCLEANFILES += $(systemd_generated_FILE)

# install-data-local: $(systemd_DATA)
#	mkdir -p $(DESTDIR)$(systemddir)
#	install -m 644 $(systemd_DATA) $(DESTDIR)$(systemddir)
