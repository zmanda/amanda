# Makefile for sample configuration files

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am

%: %.in $(top_builddir)/config.status
	$(top_builddir)/config.status --file=$@:$<

systemddir = $(unitdir)

if WANT_SERVER
systemd_units_in_FILE = \
    amanda-server.socket.in \
    amanda-server@.service.in \
    amanda-lserver.socket.in \
    amanda-lserver@.service.in \
    amanda-lserver-unix.socket.in \
    amanda-lserver-unix@.service.in \
    amanda-rest-server.service.in
else
systemd_units_in_FILE = \
    amanda-client.socket.in \
    amanda-client@.service.in \
    amanda-lclient-unix.socket.in \
    amanda-lclient-unix@.service.in

endif

systemd_presetdir = $(systemddir)-preset
systemd_preset_in_FILE = 01-amanda-units.preset

#### local-only (higher security) sockets
#### NOTE: currently cannot function as "REUSE_PORTS" fails

systemd_generated_FILE = \
   $(systemd_units_in_FILE:%.in=%)

systemd_DATA = $(systemd_generated_FILE)
systemd_preset_DATA = $(systemd_preset_in_FILE)

EXTRA_DIST += $(systemd_units_in_FILE) $(systemd_preset_in_FILE)
DISTCLEANFILES += $(systemd_generated_FILE)

