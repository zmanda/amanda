/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw ( match_disk match_host );
use Amanda::Logfile;
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
	hv_store_const(h, "client_port", safe_newSVpv(dp->client_port));
	hv_store_const(h, "ssh_keys", safe_newSVpv(dp->ssh_keys));
	hv_store_const(h, "ssl_fingerprint_file", safe_newSVpv(dp->ssl_fingerprint_file));
	hv_store_const(h, "ssl_cert_file", safe_newSVpv(dp->ssl_cert_file));
	hv_store_const(h, "ssl_key_file", safe_newSVpv(dp->ssl_key_file));
	hv_store_const(h, "ssl_ca_cert_file", safe_newSVpv(dp->ssl_ca_cert_file));
	hv_store_const(h, "ssl_cipher_list", safe_newSVpv(dp->ssl_cipher_list));
	hv_store_const(h, "ssl_check_certificate_host", newSViv(dp->ssl_check_certificate_host));
	hv_store_const(h, "ssl_check_host", newSViv(dp->ssl_check_host));
	hv_store_const(h, "ssl_check_fingerprint", newSViv(dp->ssl_check_fingerprint));
	hv_store_const(h, "auth", safe_newSVpv(dp->auth));
	hv_store_const(h, "maxdumps", newSViv(dp->host->maxdumps));
	hv_store_const(h, "disks", newRV_noinc((SV *)newAV()));

	/* and make a link to the relevant interface object */
	if (dp->host->netif) {
	    ref = get_iface(ifaces, ifaceclass, dp->host->netif);
	    SvREFCNT_inc(ref);
	    hv_store_const(h, "interface", ref);
	}
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
	char *filename)
{
    cfgerr_level_t errlev;
    disklist_t list;

    errlev = read_diskfile(filename, &list);
    return errlev;
}

static void
reset_disklist_internal(
	HV *disks, char *diskclass,
	HV *hosts, char *hostclass,
	HV *ifaces, char *ifaceclass)
{
    cfgerr_level_t errlev;
    disklist_t list;
    GList  *dlist;
    disk_t *dp;
    HV *diskclass_stash = NULL;
    am_host_t *hp;

    hv_clear(hosts);
    hv_clear(disks);
    hv_clear(ifaces);

    for (hp = get_hostlist(); hp != NULL; hp = hp->next) {
      for (dp = hp->disks; dp != NULL; dp = dp->hostnext) {
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
	hv_store_const(h, "todo", newSViv(dp->todo));

	if (dp->dtype_name) {
	    hv_store_const(h, "config",
	      SvREFCNT_inc(
	        SWIG_NewPointerObj(lookup_dumptype(dp->dtype_name),
			           /* can't use $descriptor here.. */
			           SWIGTYPE_p_dumptype_t, 0)));
	}

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
    }
    /* free_disklist frees the globals, too, which is not what we want.
     * So this leaks memory. */
    /* free_disklist(&list); */
}
%}

%inline %{
void
unload_disklist_internal(void) {
    unload_disklist();
}
%}

%perlcode %{

sub match_disklist {
    my %params = @_;
    my $user_msg = $params{'user_msg'};
    my $exact_match = $params{'exact_match'};
    my @args;
    @args = @{$params{'args'}} if defined $params{'args'};

    return if !@args;

    if ($exact_match) {
	@args = map { /^=/?$_:"=$_" } @args;
    }

    for my $disk (Amanda::Disklist::all_disks()) {
	if ($disk->{'todo'} == 1) {
	    $disk->{'todo'} = -1;
	}
    }

    my $prev_match = 0;
    my $prevhost = undef;
    for my $arg (@args) {
	my $match_a_host = 0;
	for my $hostname (sort keys %hosts) {
	    if (match_host($arg, $hostname)) {
		$match_a_host = 1;
		next;
	    }
	}

	my $match_a_disk = 0;
	my $disk_skip;
	for my $disk (Amanda::Disklist::all_disks()) {
	    if ($prevhost and
		match_host($prevhost, $disk->{'host'}->{'hostname'}) and
		(match_disk($arg, $disk->{'name'}) or
		 ($disk->{'device'} and match_disk($arg, $disk->{'device'})))) {
		if ($match_a_host) {
		    return $user_msg->(Amanda::Disklist::Message->new(
					source_filename => __FILE__,
					source_line     => __LINE__,
					code  => 1400000,
					severity => $Amanda::Message::ERROR,
					arg   => $arg));
		} else {
		    if ($disk->{'todo'} == -1) {
			$disk->{'todo'} = 1;
			$match_a_disk = 1;
			$prev_match = 0;
		    } elsif ($disk->{'todo'} == 0) {
			$match_a_disk = 1;
			$prev_match = 0;
			$disk_skip = $disk;
		    } else { # $disk->{'todo'} == 1
			$match_a_disk = 1;
			$prev_match = 0;
		    }
		}
	    }
	}

	if (!$match_a_disk) {
	    if ($match_a_host == 1) {
		if ($prev_match == 1) { # all disk of the previous host
		    for my $disk (Amanda::Disklist::all_disks()) {
			if (match_host($prevhost, $disk->{'host'}->{'hostname'})) {
			    $disk->{'todo'} = 1;
			    $match_a_disk = 1;
			}
		    }
		    if (!match_a_disk) {
			$user_msg->(Amanda::Disklist::Message->new(
					source_filename => __FILE__,
					source_line     => __LINE__,
					code  => 1400001,
					severity => $Amanda::Message::WARNING,
					host  => $prevhost));
		    }
		}
		$prevhost = $arg;
		$prev_match = 1;
	    } else {
		my $incorrect_quoting = $arg =~ /\\/;
		$user_msg->(Amanda::Disklist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code  => 1400002,
				severity => $Amanda::Message::WARNING,
				arg   => $arg,
				host  => $prevhost,
				incorrect_quoting => $incorrect_quoting));
		$prev_match = 0;
	    }
	} elsif ($disk_skip) {
	    $strategy = dumptype_getconf($dle->{config}, $DUMPTYPE_STRATEGY);
	    if ($strategy == $DS_SKIP) {
		$user_msg->(Amanda::Disklist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code  => 1400003,
				severity => $Amanda::Message::WARNING,
				arg   => $arg));
	    } else {
		$user_msg->(Amanda::Disklist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code  => 1400004,
				severity => $Amanda::Message::WARNING,
				arg   => $arg));
	    }
	    $prev_match = 0;
	}
    }

    if ($prev_match == 1) { # all disk of the previous host
	$match_a_disk = 0;
	for my $disk (Amanda::Disklist::all_disks()) {
	    if (match_host($prevhost, $disk->{'host'}->{'hostname'})) {
		$disk->{'todo'} = 1;
		$match_a_disk = 1;
	    }
	}

	if (!$match_a_disk) {
	    $user_msg->(Amanda::Disklist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code  => 1400005,
				severity => $Amanda::Message::WARNING,
				host   => $prevhost));
	}
    }

    for my $disk (Amanda::Disklist::all_disks()) {
	if ($disk->{'todo'} == -1) {
	    $disk->{'todo'} = 0;
	}
    }
}

sub do_on_match_disklist {
    my %params = @_;
    my $user_msg = $params{'user_msg'};
    my $disk_cb = $params{'disk_cb'};

    Amanda::Disklist::match_disklist(%params);

    for my $disk (Amanda::Disklist::all_disks()) {
	if ($disk->{'todo'} == 1) {
	    my @result_messages = $disk_cb->($disk);
	    for my $message (@result_messages) {
		$user_msg->($message);
	    }
	}
    }
}

%}

%perlcode %{

package Amanda::Disklist::Disk;
# methods
use Amanda::Config qw( :getconf);

%}

%newobject xml_optionstr_disk;
char *xml_optionstr_disk(char *hostname, char *diskname);

%newobject xml_application_disk;
char *xml_application_disk(char *hostname, char *diskname, am_feature_t *their_features);

%newobject xml_estimate_disk;
char *xml_estimate_disk(char *hostname, char *diskname, am_feature_t *their_features);

%perlcode %{

sub xml_optionstr {
    my $disk = shift;

    return Amanda::Disklist::xml_optionstr_disk($disk->{'host'}->{'hostname'},
			      $disk->{'name'});
}

sub xml_application {
    my $disk = shift;
    my $their_features = shift;

    return Amanda::Disklist::xml_application_disk($disk->{'host'}->{'hostname'},
			      $disk->{'name'}, $their_features->am_features());
}

sub xml_estimate {
    my $disk = shift;
    my $their_features = shift;

    return Amanda::Disklist::xml_estimate_disk($disk->{'host'}->{'hostname'},
			      $disk->{'name'}, $their_features->am_features());
}

package Amanda::Disklist::Host;

sub get_disk {
    my ($self, $disk) = @_;
    return $Amanda::Disklist::disks{$self->{'hostname'}}{$disk};
}

sub all_disks {
    my ($self) = @_;
    return sort { $a->{'name'} cmp $b->{'name'} } values %{$Amanda::Disklist::disks{$self->{'hostname'}}};
}

package Amanda::Disklist::Interface;

# methods

package Amanda::Disklist;

our (%disks, %hosts, %interfaces);

sub read_disklist {
    my %params = @_;

    my $errlev = read_disklist_internal(
	($params{filename} or config_dir_relative(getconf($CNF_DISKFILE))));
    if ($errlev >= $CFGERR_ERRORS) {
	return $errlev;
    }
    reset_disklist_internal(
	\%disks, ($params{disk_class} or "Amanda::Disklist::Disk"),
	\%hosts, ($params{host_class} or "Amanda::Disklist::Host"),
	\%interfaces, ($params{interface_class} or "Amanda::Disklist::Interface"),
    );
    return $errlev;
}

sub reset_disklist {
    reset_disklist_internal(
	\%disks, ($params{disk_class} or "Amanda::Disklist::Disk"),
	\%hosts, ($params{host_class} or "Amanda::Disklist::Host"),
	\%interfaces, ($params{interface_class} or "Amanda::Disklist::Interface"));
}

sub add_holding_to_disklist {
    Amanda::Logfile::search_holding_disk();
    reset_disklist_internal(
	\%disks, ($params{disk_class} or "Amanda::Disklist::Disk"),
	\%hosts, ($params{host_class} or "Amanda::Disklist::Host"),
	\%interfaces, ($params{interface_class} or "Amanda::Disklist::Interface"));
}

sub unload_disklist {
    return unload_disklist_internal();
}

sub get_host {
    my ($hostname) = @_;
    return $hosts{$hostname};
}

sub all_hosts {
    return sort { $a->{'hostname'} cmp $b->{'hostname'} } values %hosts;
}

sub get_disk {
    my ($hostname, $diskname) = @_;
    return $disks{$hostname}->{$diskname};
}

sub all_disks {
    my @rv;
    foreach my $hostname (sort keys %disks) {
	foreach my $diskname ( sort keys %{$disks{$hostname}} ) {
	    push @rv, $disks{$hostname}->{$diskname};
	}
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

%perlcode %{
package Amanda::Disklist::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 1400000) {
	return "Argument '$self->{'arg'}' cannot be both a host and a disk.";
    } elsif ($self->{'code'} == 1400001) {
	return "All disks on host '$self->{'host'}' are ignored or have strategy \"skip\".";
    } elsif ($self->{'code'} == 1400002) {
	return "Argument '$self->{'arg'}' matches neither a host nor a disk". (($self->{'incorrect_quoting'}) ? "; quoting may be incorrect.":".");
    } elsif ($self->{'code'} == 1400003) {
	return "Argument '$self->{'arg'}' matches a disk with strategy \"skip\"";
    } elsif ($self->{'code'} == 1400004) {
	return "Argument '$self->{'arg'}' matches a disk marked \"ignore\"";
    } elsif ($self->{'code'} == 1400005) {
	return "All disks on host '$self->{'host'}' are ignored or have strategy \"skip\".";
    } elsif ($self->{'code'} == 1400006) {
	return "Errors processing disklist '$self->{'diskfile'}'.";
    } elsif ($self->{'code'} == 1400007) {
	return "No such host '$self->{'host'}' in disklist.";
    } elsif ($self->{'code'} == 1400008) {
	return "No such disk '$self->{'disk'}' for host '$self->{'host'}' in disklist.";
    } elsif ($self->{'code'} == 1400009) {
	return "Required 'disk' argument is not provided.";
    } elsif ($self->{'code'} == 1400010) {
	return "List of DLEs.";
    }
}
%}

