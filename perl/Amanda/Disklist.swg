/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

%module "Amanda::Disklist"
%include "amglue/amglue.swg"
%include "exception.i"
%import "Amanda/Config.swg"

%include "Amanda/Disklist.pod"

%{
#include "conffile.h"
#include "diskfile.h"
#include "amglue.h"
%}

%perlcode %{
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :getconf config_dir_relative );
%}

/* handle creation of opaque objects from Amanda::Config */
%types(interface_t *, dumptype_t *);

%{
/* Utility functions for read_disklist_internal */
#define hv_store_const(h, k, v) hv_store((h), (k), sizeof((k))-1, (v), 0)
#define safe_newSVpv(str) ((str)? newSVpv((str), 0) : &PL_sv_undef)

static SV *
get_iface(
    HV *ifaces,
    char *ifaceclass,
    netif_t *iface)
{
    char *name = interface_name(iface->config);
    HV *ifaceclass_stash = NULL;
    SV **svp;

    svp = hv_fetch(ifaces, name, strlen(name), TRUE);
    if (!svp)
	croak("internal error"); /* shouldn't happen with lval = TRUE */

    if (!SvOK(*svp)) {
	/* no existing iface, so set up a new one */
	HV *h;
	SV *ref;

	/* make a new hashref and bless it */
	h = newHV();
	ref = newRV_noinc((SV *)h);
	if (!ifaceclass_stash) {
	    ifaceclass_stash = gv_stashpv(ifaceclass, GV_ADD);
	}
	sv_bless(ref, ifaceclass_stash);
	sv_setsv(*svp, ref);

	/* fill in the relevant value - a reference to the config */
	hv_store_const(h, "config",
	  SvREFCNT_inc(
	    SWIG_NewPointerObj(iface->config,
			       /* can't use $descriptor here.. */
			       SWIGTYPE_p_interface_t, 0)));
    }

    return *svp;
}

static SV *
get_host(
    HV *hosts,
    char *hostclass,
    HV *ifaces,
    char *ifaceclass,
    disk_t *dp)
{
    HV *hostclass_stash = NULL;
    SV **svp;
    SV *sv;
    AV *disksarray;

    svp = hv_fetch(hosts, dp->hostname, strlen(dp->hostname), TRUE);
    if (!svp)
	croak("internal error"); /* shouldn't happen with lval = TRUE */

    if (SvOK(*svp)) {
	/* this host already exists */
    } else {
	/* no existing host, so set up a new one */
	HV *h;
	SV *ref;

	/* make a new hashref and bless it */
	h = newHV();
	ref = newRV_noinc((SV *)h);
	g_assert(SvREFCNT((SV *)h) == 1);
	if (!hostclass_stash) {
	    hostclass_stash = gv_stashpv(hostclass, GV_ADD);
	}
	sv_bless(ref, hostclass_stash);
	sv_setsv(*svp, ref);
	g_assert(SvREFCNT(*svp) == 1);

	/* fill in the relevant values */
	hv_store_const(h, "hostname", safe_newSVpv(dp->hostname));
	hv_store_const(h, "amandad_path", safe_newSVpv(dp->amandad_path));
	hv_store_const(h, "client_username", safe_newSVpv(dp->client_username));
	hv_store_const(h, "ssh_keys", safe_newSVpv(dp->ssh_keys));
	hv_store_const(h, "auth", safe_newSVpv(dp->auth));
	hv_store_const(h, "maxdumps", newSViv(dp->host->maxdumps));
	hv_store_const(h, "disks", newRV_noinc((SV *)newAV()));

	/* and make a link to the relevant interface object */
	ref = get_iface(ifaces, ifaceclass, dp->host->netif);
	SvREFCNT_inc(ref);
	hv_store_const(h, "interface", ref);
    }

    /* push the name of the disk into @{$self->{disks}}; we don't store
     * a ref to the disk object, as that would create a circular link */
    g_assert(SvROK(*svp));
    g_assert(SvTYPE(SvRV(*svp)) == SVt_PVHV);
    sv = *hv_fetch((HV *)SvRV(*svp), "disks", sizeof("disks")-1, 0);

    g_assert(SvROK(sv));
    g_assert(SvTYPE(SvRV(sv)) == SVt_PVAV);
    av_push((AV *)SvRV(sv), safe_newSVpv(dp->name));

    return *svp;
}
%}

/* typemaps to pass AV's and HV's in directly (as perl refs) */
%typemap(in) HV * {
    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVHV) {
	SWIG_exception_fail(SWIG_TypeError, "must provide a hashref");
    }

    $1 = (HV *)SvRV($input);
}

%typemap(in) AV * {
    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
	SWIG_exception_fail(SWIG_TypeError, "must provide an arrayref");
    }

    $1 = (AV *)SvRV($input);
}

%inline %{
static cfgerr_level_t
read_disklist_internal(
	char *filename,
	HV *disks, char *diskclass,
	HV *hosts, char *hostclass,
	HV *ifaces, char *ifaceclass)
{
    cfgerr_level_t errlev;
    disklist_t list;
    disk_t *dp;
    HV *diskclass_stash = NULL;

    hv_clear(hosts);
    hv_clear(disks);
    hv_clear(ifaces);

    errlev = read_diskfile(filename, &list);
    if (errlev >= CFGERR_ERRORS)
	return errlev;

    for (dp = list.head; dp; dp = dp->next) {
	HV *h = newHV();
	SV *diskref;
	SV *hostref;
	SV **svp;

	/* make a new hashref and bless it */
	h = newHV();
	diskref = newRV_noinc((SV *)h);
	if (!diskclass_stash) {
	    diskclass_stash = gv_stashpv(diskclass, GV_ADD);
	}
	sv_bless(diskref, diskclass_stash);

	hv_store_const(h, "name", safe_newSVpv(dp->name));
	hv_store_const(h, "device", safe_newSVpv(dp->device));
	hv_store_const(h, "spindle", newSViv(dp->spindle));

	hv_store_const(h, "config",
	  SvREFCNT_inc(
	    SWIG_NewPointerObj(lookup_dumptype(dp->dtype_name),
			       /* can't use $descriptor here.. */
			       SWIGTYPE_p_dumptype_t, 0)));

	/* create an uplink ref to the host object */
	hostref = get_host(hosts, hostclass, ifaces, ifaceclass, dp);
	SvREFCNT_inc(hostref);
	hv_store_const(h, "host", hostref);

	/* and store this disk in the two-level %disks hash */
	svp = hv_fetch(disks, dp->hostname, strlen(dp->hostname), 1);
	if (!SvOK(*svp)) {
	    /* make a new hash for this host */
	    SV *href;
	    h = newHV();
	    href = newRV_noinc((SV *)h);
	    sv_setsv(*svp, href);
	} else {
	    g_assert(SvROK(*svp));
	    g_assert(SvTYPE(SvRV(*svp)) == SVt_PVHV);
	    h = (HV *)SvRV(*svp);
	}

	g_assert(SvROK(*svp));
	g_assert(SvTYPE(SvRV(*svp)) == SVt_PVHV);
	SvREFCNT_inc(diskref);
	hv_store(h, dp->name, strlen(dp->name), diskref, 0);
    }

    /* free_disklist frees the globals, too, which is not what we want.
     * So this leaks memory. */
    /* free_disklist(&list); */

    return 0;
}
%}

%perlcode %{

package Amanda::Disklist::Disk;

# methods

package Amanda::Disklist::Host;

sub get_disk {
    my ($self, $disk) = @_;
    return $Amanda::Disklist::disks{$self->{'hostname'}}{$disk};
}

sub all_disks {
    my ($self) = @_;
    return values %{$Amanda::Disklist::disks{$self->{'hostname'}}};
}

package Amanda::Disklist::Interface;

# methods

package Amanda::Disklist;

our (%disks, %hosts, %interfaces);

sub read_disklist {
    my %params = @_;

    return read_disklist_internal(
	($params{filename} or config_dir_relative(getconf($CNF_DISKFILE))),
	\%disks, ($params{disk_class} or "Amanda::Disklist::Disk"),
	\%hosts, ($params{host_class} or "Amanda::Disklist::Host"),
	\%interfaces, ($params{interface_class} or "Amanda::Disklist::Interface"),
    );
}

sub get_host {
    my ($hostname) = @_;
    return $hosts{$hostname};
}

sub all_hosts {
    return values %hosts;
}

sub get_disk {
    my ($hostname, $diskname) = @_;
    return $disks{$hostname}->{$diskname};
}

sub all_disks {
    my @rv;
    foreach my $disk (values %disks) {
	push @rv, (values %$disk);
    }
    return @rv;
}

sub get_interface {
    my ($interfacename) = @_;
    return $interfaces{$interfacename};
}

sub all_interfaces {
    return values %interfaces;
}

push @EXPORT_OK, qw( read_disklist
	get_host all_hosts
	get_disk all_disks
	get_interface all_interfaces);

%}
char *clean_dle_str_for_client(char *dle_str, am_feature_t *their_features);
