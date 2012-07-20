/*
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

%module "Amanda::IPC::Binary"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/IPC/Binary.pod"

%{
#include <glib.h>
#include "ipc-binary.h"
%}

/*
 * types
 */
typedef struct ipc_binary_proto_t ipc_binary_proto_t;
typedef struct ipc_binary_cmd_t ipc_binary_cmd_t;
typedef struct ipc_binary_channel_t ipc_binary_channel_t;
typedef struct ipc_binary_message_t ipc_binary_message_t;

/*
 * typemaps
 */

%typemap(out) ipc_binary_message_t * {
    static HV *amanda_xfer_msg_stash = NULL;
    HV *hash;
    SV *rv;
    AV *args;
    int i, nargs;

    if ($1) {
	hash = newHV();
	rv = newRV_noinc((SV *)hash);

	/* bless the rv as an Amanda::Xfer::Msg object */
	if (!amanda_xfer_msg_stash) {
	    amanda_xfer_msg_stash = gv_stashpv("Amanda::IPC::Binary::Message", GV_ADD);
	}
	sv_bless(rv, amanda_xfer_msg_stash);

	args = newAV();
	hv_store(hash, "cmd_id", 6, newSViv($1->cmd_id), 0);
	hv_store(hash, "args", 4, newRV_noinc((SV *)args), 0);

	/* loop over all messages, using av_store to insert the args which are present;
	* this will fill in undef's where necessary */
	for (i = 0; i < $1->n_args; i++) {
	    if ($1->args[i].data == NULL)
		continue;

	    g_assert(NULL !=
		av_store(args, i, newSVpvn($1->args[i].data, $1->args[i].len)));
	}

	/* we don't need the C data any more */
	ipc_binary_free_message($1);

	$result = rv;
	argvi++;
    }
}

%typemap(in) ipc_binary_message_t * {
    HV *hv;
    AV *av;
    SV **svp;
    int cmd_id;
    ipc_binary_channel_t *chan = NULL;
    ipc_binary_message_t *msg;
    int i, len;

    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVHV
	    || !sv_isa($input, "Amanda::IPC::Binary::Message"))
	SWIG_exception(SWIG_TypeError, "Expected an Amanda::IPC::Binary::Message");

    hv = (HV *)SvRV($input);

    /* get cmd_id */
    svp = hv_fetch(hv, "cmd_id", 6, FALSE);
    if (!svp || !SvIOK(*svp))
	SWIG_exception(SWIG_TypeError, "'cmd_id' key missing or not numeric");
    cmd_id = SvIV(*svp);

    /* get channel */
    svp = hv_fetch(hv, "chan", 4, FALSE);
    if (!svp || SWIG_ConvertPtr(*svp, (void **)&chan,
				$descriptor(ipc_binary_channel_t *), 0) == -1
	     || !chan)
	SWIG_exception(SWIG_TypeError, "'chan' key missing or incorrect");

    /* get args */
    svp = hv_fetch(hv, "args", 4, FALSE);
    if (!svp || !SvROK(*svp) || SvTYPE(SvRV(*svp)) != SVt_PVAV)
	SWIG_exception(SWIG_TypeError, "'args' key missing or not an arrayref");
    av = (AV *)SvRV(*svp);

    msg = ipc_binary_new_message(chan, cmd_id);

    len = av_len(av);
    for (i = 0; i <= len; i++) {
	SV **elt = av_fetch(av, i, 0);
	STRLEN datasize;
	gpointer data;

	if (elt && SvPOK(*elt)) {
	    data = (gpointer)SvPV(*elt, datasize);
	    ipc_binary_add_arg(msg, i, datasize, data, 0);
	}
    }

    $1 = msg;
}

/*
 * functions
 */

ipc_binary_proto_t *ipc_binary_proto_new(
    guint16 magic);

ipc_binary_cmd_t *ipc_binary_proto_add_cmd(
    ipc_binary_proto_t *proto,
    guint16 id);

void ipc_binary_cmd_add_arg(
    ipc_binary_cmd_t *cmd,
    guint16 id,
    guint8 flags);

/* flag symbols for use in perl; values don't matter */
enum {
    IPC_BINARY_STRING,
    IPC_BINARY_OPTIONAL,
};
amglue_export(
    $IPC_BINARY_STRING $IPC_BINARY_OPTIONAL);

ipc_binary_channel_t *ipc_binary_new_channel(
    ipc_binary_proto_t *proto);

void ipc_binary_free_channel(
    ipc_binary_channel_t *channel);

ipc_binary_message_t *ipc_binary_read_message(
    ipc_binary_channel_t *chan,
    int fd);

int ipc_binary_write_message(
    ipc_binary_channel_t *chan,
    int fd,
    ipc_binary_message_t *msg);

void ipc_binary_feed_data(
    ipc_binary_channel_t *chan,
    gsize size,
    gpointer data);

void ipc_binary_data_transmitted(
    ipc_binary_channel_t *chan,
    gsize size);

ipc_binary_message_t *ipc_binary_poll_message(
    ipc_binary_channel_t *chan);

void ipc_binary_queue_message(
    ipc_binary_channel_t *chan,
    ipc_binary_message_t *msg);


/*
 * Perl layer
 */

%perlcode %{

use Carp;
push @EXPORT, qw( magic command new message );

# a map from package name to protocol
my %protos_by_pkg;

sub magic {
    my ($magic) = @_;
    my $caller = caller;

    croak "magic already set for this protocol"
	if (exists $protos_by_pkg{$caller});

    $protos_by_pkg{$caller} = ipc_binary_proto_new($magic);
}

sub command {
    my ($cmd_id, @args) = @_;
    my $caller = caller;

    croak "magic not set for this protocol"
	unless (exists $protos_by_pkg{$caller});

    croak "command args must be specified in pairs"
	unless (@args % 2 == 0);

    my $proto = $protos_by_pkg{$caller};
    $cmd = ipc_binary_proto_add_cmd($proto, $cmd_id);

    while (@args) {
	my $arg = shift @args;
	my $flags = shift @args;
	ipc_binary_cmd_add_arg($cmd, $arg, $flags);
    }
}

##
# Class Methods

sub new {
    my $class = shift;

    my $self = bless {
	chan => ipc_binary_new_channel($protos_by_pkg{$class}),
    }, $class;
}

sub message {
    my $self = shift;
    my ($cmd_id, @args) = @_;

    $self = bless {
	cmd_id => $cmd_id,
	chan => $self->{'chan'},
	args => [],
    }, "Amanda::IPC::Binary::Message";


    while (@args) {
	my $arg = shift @args;
	my $val = shift @args;
	$self->{'args'}[$arg] = $val;
    }

    return $self;
}

sub close {
    if ($self->{'chan'}) {
	ipc_binary_free_channel($self->{'chan'});
	$self->{'chan'} = undef;
    }
}

*DESTROY = *close;

##
# Blocking interface

sub read_message {
    my $self = shift;
    my ($fd) = @_;

    return ipc_binary_read_message($self->{'chan'}, $fd);
}

sub write_message {
    my $self = shift;
    my ($fd, $msg) = @_;

    if (ipc_binary_write_message($self->{'chan'}, $fd, $msg) < 0) {
	return 0;
    }
    return 1;
}

##
# Nonblocking interface -- TODO

##
# Message structure

package Amanda::IPC::Binary::Message;

# (constructor is the protocol's C<message> method)

# format:
# { cmd_id => $cmd_id,
#   chan => $channel,
#   args => [ $arg0, $arg1, .. ],
# }

sub get_cmd {
    return $self->{'cmd_id'};
}

sub get_arg {
    my ($self, $arg_id) = @_;

    return $self->{'args'}[$arg_id];
}

sub set_arg {
    my ($self, $arg_id, $value) = @_;
    $self->{'args'}[$arg_id] = $value;
}

package Amanda::IPC::Binary;

%}
