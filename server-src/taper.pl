#! @PERL@
# Copyright (c) 2009, 2010 Zmanda Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

package main::Protocol;

use Amanda::IPC::LineProtocol;
use base "Amanda::IPC::LineProtocol";

use constant START_TAPER => message("START-TAPER",
    format => [ qw( timestamp ) ],
);

use constant PORT_WRITE => message("PORT-WRITE",
    format => [ qw( handle hostname diskname level datestamp splitsize
		    split_diskbuffer fallback_splitsize ) ],
);

use constant FILE_WRITE => message("FILE-WRITE",
    format => [ qw( handle filename hostname diskname level datestamp splitsize ) ],
);

use constant NEW_TAPE => message("NEW-TAPE",
    format => {
	in => [ qw( ) ],
	out => [ qw( handle label ) ],
    },
);

use constant NO_NEW_TAPE => message("NO-NEW-TAPE",
    format => {
	in => [ qw( reason ) ],
	out => [ qw( handle ) ],
    }
);

use constant FAILED => message("FAILED",
    format => {
	in => [ qw( handle ) ],
	out => [ qw( handle input taper inputerr tapererr ) ],
    },
);

use constant DONE => message("DONE",
    format => {
	in => [ qw( handle ) ],
	out => [ qw( handle input taper stats inputerr tapererr ) ],
    },
);

use constant QUIT => message("QUIT",
    on_eof => 1,
);

use constant TAPER_OK => message("TAPER-OK",
);

use constant TAPE_ERROR => message("TAPE-ERROR",
    format => [ qw( handle message ) ],
);

use constant PARTIAL => message("PARTIAL",
    format => [ qw( handle input taper stats inputerr tapererr ) ],
);

use constant PARTDONE => message("PARTDONE",
    format => [ qw( handle label fileno kb stats ) ],
);

use constant REQUEST_NEW_TAPE => message("REQUEST-NEW-TAPE",
    format => [ qw( handle ) ],
);

use constant PORT => message("PORT",
    format => [ qw( port ipports ) ],
);

use constant BAD_COMMAND => message("BAD-COMMAND",
    format => [ qw( message ) ],
);

use constant DUMPER_STATUS => message("DUMPER-STATUS",
    format => [ qw( handle ) ],
);


package main::Controller;

use IO::Socket;
use POSIX qw( :errno_h );
use Amanda::Changer;
use Amanda::Config qw( :getconf config_dir_relative );
use Amanda::Header;
use Amanda::Holding;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::MainLoop;
use Amanda::Taper::Scan;
use Amanda::Taper::Scribe;
use Amanda::Logfile qw( :logtype_t log_add );
use Amanda::Xfer qw( :constants );
use Amanda::Util qw( quote_string );
use Amanda::Tapelist;
use File::Temp;

use base qw( Amanda::Taper::Scribe::Feedback );

sub new {
    my $class = shift;

    my $self = bless {
	state => "init",

	# filled in at start
	proto => undef,
	scribe => undef,
	header_socket => undef,
	header_socket_src => undef,
	header_socket_connect_cb => undef,
	tape_num => 0,

	# filled in when a write starts:
        xfer => undef,
	xfer_source => undef,
	handle => undef,
        hostname => undef,
        diskname => undef,
        datestamp => undef,
        level => undef,
	header => undef,
	last_partnum => -1,
	doing_port_write => undef,

	# periodic status updates
	timer => undef,
	status_filename => undef,
	status_fh => undef,

        # filled in after the header is available
	header => undef,

	# filled in when a new tape is started:
	label => undef,
    }, $class;
    return $self;
}

# The feedback object mediates between messages from the driver and the ongoing
# action with the taper.  This is made a little bit complicated because the
# driver conversation is fairly contextual, with some responses answering
# "questions" asked earlier.  This is modeled with the following taper
# "states":
#
# init:
#   waiting for START-TAPER command
# starting:
#   warming up devices; TAPER-OK not sent yet
# idle:
#   not currently dumping anything
# making_xfer:
#   setting up a transfer for a new dump
# getting_header:
#   getting the header before beginning a new dump
# writing:
#   in the middle of writing a file (self->{'handle'} set)
# error:
#   a fatal error has occurred, so this object won't do anything

sub start {
    my $self = shift;

    $self->_assert_in_state("init") or return;

    my $message_cb = make_cb(message_cb => sub {
	my ($msgtype, %params) = @_;
	my $msg;
	if (defined $msgtype) {
	    $msg = "unhandled command '$msgtype'";
	} else {
	    $msg = $params{'error'};
	}
	log_add($L_ERROR, $msg);
	print STDERR "$msg\n";
	$self->{'proto'}->send(main::Protocol::BAD_COMMAND,
	    message => $msg);
    });
    $self->{'proto'} = main::Protocol->new(
	rx_fh => *STDIN,
	tx_fh => *STDOUT,
	message_cb => $message_cb,
	message_obj => $self,
	debug => $Amanda::Config::debug_taper?'driver/taper':'',
    );

    my $changer = Amanda::Changer->new();
    if ($changer->isa("Amanda::Changer::Error")) {
	# send a TAPE_ERROR right away
	$self->{'proto'}->send(main::Protocol::TAPE_ERROR,
		handle => '99-9999', # fake handle
		message => "$changer");

	# wait for it to be transmitted, then exit
	$self->{'proto'}->stop(finished_cb => sub {
	    Amanda::MainLoop::quit();
	});

	# don't finish start()ing
	return;
    }

    my $taperscan = Amanda::Taper::Scan->new(changer => $changer);
    $self->{'scribe'} = Amanda::Taper::Scribe->new(
	taperscan => $taperscan,
	feedback => $self,
	debug => $Amanda::Config::debug_taper);

    # set up a listening socket on an arbitrary port
    my $sock = $self->{'header_socket'} = IO::Socket::INET->new(
	LocalHost => "127.0.0.1",
	Proto => "tcp",
	Listen => 1,
	ReuseAddr => 1,
	Blocking => 0,
    );
    $sock->listen(5);

    # arrange to call header_socket_connect_cb when a new connection comes in
    $self->{'header_socket_src'} =
	Amanda::MainLoop::fd_source($sock->fileno(), $G_IO_IN);
    $self->{'header_socket_src'}->set_callback(sub {
	my ($src) = @_;
	# this is called when a new connection comes in on the header_socket, and calls
	# through to $self->{'header_socket_connect_cb'}, or rejects the connection if
	# there is no such callback running

	my $child_sock = $self->{'header_socket'}->accept();

	# and make sure it's set to blocking mode
	$child_sock->blocking(1);

	if ($self->{'header_socket_connect_cb'}) {
	    my $cb = $self->{'header_socket_connect_cb'};
	    # reset the callback first to avoid double-calling it
	    $self->{'header_socket_connect_cb'} = undef;
	    return $cb->($child_sock);
	} else {
	    # reject connections when we haven't sent a PORT request
	    $child_sock->close();
	}
    });
}

# called when the scribe is fully started up and ready to go
sub _scribe_started_cb {
    my $self = shift;
    my ($err) = @_;

    if ($err) {
	$self->{'proto'}->send(main::Protocol::TAPE_ERROR,
		handle => '99-9999', # fake handle
		message => "$err");
	$self->{'state'} = "error";
    } else {
	$self->{'proto'}->send(main::Protocol::TAPER_OK);
	$self->{'state'} = "idle";
    }
}

sub quit {
    my $self = shift;
    my %params = @_;
    my %subs;
    my @errors = @_;

    $subs{'stop_socket'} = make_cb(stop_socket => sub {
	$self->{'header_socket_src'}->remove();
	$self->{'header_socket'}->close();

	$subs{'quit_scribe'}->();
    });

    $subs{'quit_scribe'} = make_cb(quit_scribe => sub {
	$self->{'scribe'}->quit(finished_cb => sub {
	    my ($err) = @_;
	    push @errors, $err if ($err);

	    $subs{'stop_proto'}->();
	});
    });

    $subs{'stop_proto'} = make_cb(stop_proto => sub {
	$self->{'proto'}->stop(finished_cb => sub {
	    my ($err) = @_;
	    push @errors, $err if ($err);

	    $subs{'done'}->();
	});
    });

    $subs{'done'} = make_cb(done => sub {
	if (@errors) {
	    $params{'finished_cb'}->(join("; ", @errors));
	} else {
	    $params{'finished_cb'}->();
	}
    });

    $subs{'stop_socket'}->();
}

##
# Scribe feedback

sub request_volume_permission {
    my $self = shift;
    my %params = @_;

    # set up callbacks from when we hear back from the driver
    my $new_tape_cb = make_cb(new_tape_cb => sub {
	my ($msgtype, %msg_params) = @_;
	$params{'perm_cb'}->(undef);
    });
    $self->{'proto'}->set_message_cb(main::Protocol::NEW_TAPE,
	$new_tape_cb);

    my $no_new_tape_cb = make_cb(no_new_tape_cb => sub {
	my ($msgtype, %msg_params) = @_;

	# log the error (note that the message is intentionally not quoted)
	log_add($L_ERROR, "no-tape [$msg_params{reason}]");

	$params{'perm_cb'}->($msg_params{'reason'});
    });
    $self->{'proto'}->set_message_cb(main::Protocol::NO_NEW_TAPE,
	$no_new_tape_cb);

    # and send the request to the driver
    $self->{'proto'}->send(main::Protocol::REQUEST_NEW_TAPE,
	handle => $self->{'handle'});
}

sub notif_new_tape {
    my $self = shift;
    my %params = @_;

    # TODO: if $params{error} is set, report it back to the driver
    # (this will be a change to the protocol)
    if ($params{'volume_label'}) {
	$self->{'label'} = $params{'volume_label'};

	# register in the tapelist
	my $tl_file = config_dir_relative(getconf($CNF_TAPELIST));
	my $tl = Amanda::Tapelist::read_tapelist($tl_file);
	my $tle = $tl->lookup_tapelabel($params{'volume_label'});
	$tl->remove_tapelabel($params{'volume_label'});
	$tl->add_tapelabel($self->{'timestamp'}, $params{'volume_label'},
		$tle? $tle->{'comment'} : undef);
	$tl->write($tl_file);

	# add to the trace log
	log_add($L_START, sprintf("datestamp %s label %s tape %s",
		$self->{'timestamp'},
		quote_string($self->{'label'}),
		++$self->{'tape_num'}));

	# and the amdump log
	print STDERR "taper: wrote label `$self->{label}'\n";

	# and inform the driver
	$self->{'proto'}->send(main::Protocol::NEW_TAPE,
	    handle => $self->{'handle'},
	    label => $params{'volume_label'});
    } else {
	$self->{'label'} = undef;

	$self->{'proto'}->send(main::Protocol::NO_NEW_TAPE,
	    handle => $self->{'handle'});
    }
}

sub notif_part_done {
    my $self = shift;
    my %params = @_;

    $self->_assert_in_state("writing") or return;

    $self->{'last_partnum'} = $params{'partnum'};

    my $stats = $self->make_stats($params{'size'}, $params{'duration'});

    # log the part, using PART or PARTPARTIAL
    my $logbase = sprintf("%s %s %s %s %s %s/%s %s %s",
	quote_string($self->{'label'}),
	$params{'fileno'},
	quote_string($self->{'header'}->{'name'}.""), # " is required for SWIG..
	quote_string($self->{'header'}->{'disk'}.""),
	$self->{'datestamp'},
	$params{'partnum'}, -1, # totalparts is always -1
	$self->{'level'},
	$stats);
    if ($params{'successful'}) {
	log_add($L_PART, $logbase);
    } else {
	log_add($L_PARTPARTIAL, "$logbase \"No space left on device\"");
    }

    # only send a PARTDONE if it was successful
    if ($params{'successful'}) {
	$self->{'proto'}->send(main::Protocol::PARTDONE,
	    handle => $self->{'handle'},
	    label => $self->{'label'},
	    fileno => $params{'fileno'},
	    stats => $stats,
	    kb => $params{'size'} / 1024);
    }
}

sub notif_log_info {
    my $self = shift;
    my %params = @_;

    log_add($L_INFO, $params{'message'});
}

##
# Driver commands

sub msg_START_TAPER {
    my $self = shift;
    my ($msgtype, %params) = @_;

    $self->_assert_in_state("init") or return;

    $self->{'state'} = "starting";
    $self->{'scribe'}->start(dump_timestamp => $params{'timestamp'},
	finished_cb => sub { $self->_scribe_started_cb(@_); });
    $self->{'timestamp'} = $params{'timestamp'};
}

# defer both PORT_ and FILE_WRITE to a common method
sub msg_FILE_WRITE {
    my $self = shift;
    my ($msgtype, %params) = @_;

    $self->_assert_in_state("idle") or return;

    $self->{'doing_port_write'} = 0;

    $self->setup_dump($msgtype, %params);
}

sub msg_PORT_WRITE {
    my $self = shift;
    my ($msgtype, %params) = @_;
    my $read_cb;

    $self->_assert_in_state("idle") or return;

    $self->{'doing_port_write'} = 1;

    $self->setup_dump($msgtype, %params);
}

sub msg_QUIT {
    my $self = shift;
    my ($msgtype, %params) = @_;
    my $read_cb;

    # because the driver hangs up on us immediately after sending QUIT,
    # and EOF also means QUIT, we tend to get this command repeatedly.
    # So check to make sure this is only called once
    return if $self->{'quitting'};
    $self->{'quitting'} = 1;

    my $finished_cb = make_cb(finished_cb => sub {
	Amanda::MainLoop::quit();
    });
    $self->quit(finished_cb => $finished_cb);
};

##
# Utilities

sub _assert_in_state {
    my $self = shift;
    my ($state) = @_;
    if ($self->{'state'} eq $state) {
	return 1;
    } else {
	$self->{'proto'}->send(main::Protocol::BAD_COMMAND,
	    message => "command not appropriate in state '$self->{state}'");
	return 0;
    }
}

# Make up the [sec .. kb .. kps ..] section of the result messages
sub make_stats {
    my $self = shift;
    my ($size, $duration) = @_;

    $duration = 0.1 if $duration == 0;  # prevent division by zero
    my $kb = $size/1024;
    my $kps = "$kb.0"/$duration; # Perlish cast from BigInt to float

    return sprintf("[sec %f kb %d kps %f]", $duration, $kb, $kps);
}

sub create_status_file {
    my $self = shift;

    # create temporary file
    ($self->{status_fh}, $self->{status_filename}) =
	File::Temp::tempfile("taper_status_file_XXXXXX",
				DIR => $Amanda::Paths::AMANDA_TMPDIR,
				UNLINK => 1);

    # tell amstatus about it by writing it to the dump log
    my $qdisk = Amanda::Util::quote_string($self->{'diskname'});
    my $qhost = Amanda::Util::quote_string($self->{'hostname'});
    print STDERR "taper: status file $qhost $qdisk:" .
		    "$self->{status_filename}\n";
    print {$self->{status_fh}} "0";

    # create timer callback, firing every 5s (=5000msec)
    $self->{timer} = Amanda::MainLoop::timeout_source(5000);
    $self->{timer}->set_callback(sub {
	my $size = $self->{scribe}->get_bytes_written();
	seek $self->{status_fh}, 0, 0;
	print {$self->{status_fh}} $size;
	$self->{status_fh}->flush();
    });
}

# utility function for setup_dump, returning keyword args
# for $scribe->get_xfer_dest
sub get_splitting_config {
    my $self = shift;
    my ($msgtype, %params) = @_;
    my %get_xfer_dest_args;

    my $max_memory;
    if (getconf_seen($CNF_DEVICE_OUTPUT_BUFFER_SIZE)) {
        $max_memory = getconf($CNF_DEVICE_OUTPUT_BUFFER_SIZE);
    } elsif (getconf_seen($CNF_TAPEBUFS)) {
        $max_memory = getconf($CNF_TAPEBUFS) * 32768;
    } else {
        # use the default value
        $max_memory = getconf($CNF_DEVICE_OUTPUT_BUFFER_SIZE);
    }
    $get_xfer_dest_args{'max_memory'} = $max_memory;

    # here, things look a little bit different depending on whether we're
    # reading from holding (FILE_WRITE) or from a network socket (PORT_WRITE)
    if ($msgtype eq main::Protocol::FILE_WRITE) {
        if ($params{'splitsize'} ne 0) {
            $get_xfer_dest_args{'split_method'} = 'cache_inform';
            $get_xfer_dest_args{'part_size'} = $params{'splitsize'}+0;
        } else {
            $get_xfer_dest_args{'split_method'} = 'none';
        }
    } else {
        # if we have a disk buffer, use it
        if ($params{'split_diskbuffer'} ne "NULL") {
            if ($params{'splitsize'} ne 0) {
                $get_xfer_dest_args{'split_method'} = 'disk';
                $get_xfer_dest_args{'disk_cache_dirname'} = $params{'split_diskbuffer'};
                $get_xfer_dest_args{'part_size'} = $params{'splitsize'}+0;
            } else {
                $get_xfer_dest_args{'split_method'} = 'none';
            }
        } else {
            # otherwise, if splitsize is nonzero, use memory
            if ($params{'splitsize'} ne 0) {
                my $size = $params{'fallback_splitsize'}+0;
                $size = $params{'splitsize'}+0 unless ($size);
                $get_xfer_dest_args{'split_method'} = 'memory';
                $get_xfer_dest_args{'part_size'} = $size;
            } else {
                $get_xfer_dest_args{'split_method'} = 'none';
            }
        }
    }

    # implement the fallback to memory buffering if the disk buffer does
    # not exist or doesnt have enough space
    my $need_fallback = 0;
    if ($get_xfer_dest_args{'split_method'} eq 'disk') {
        if (! -d $get_xfer_dest_args{'disk_cache_dirname'}) {
            $need_fallback = "'$get_xfer_dest_args{disk_cache_dirname}' not found or not a directory";
        } else {
            my $fsusage = Amanda::Util::get_fs_usage($get_xfer_dest_args{'disk_cache_dirname'});
            my $avail = $fsusage->{'blocks'} * $fsusage->{'bavail'};
            my $dir = $get_xfer_dest_args{'disk_cache_dirname'};
            Amanda::Debug::debug("disk cache has $avail bytes available on $dir, but need $get_xfer_dest_args{part_size}");
            if ($fsusage->{'blocks'} * $fsusage->{'bavail'} < $get_xfer_dest_args{'part_size'}) {
                $need_fallback = "insufficient space in disk cache directory";
            }
        }
    }

    if ($need_fallback) {
        Amanda::Debug::warning("falling back to memory buffer for splitting: $need_fallback");
        my $size = $params{'fallback_splitsize'}+0;
        $get_xfer_dest_args{'split_method'} = 'memory';
        $get_xfer_dest_args{'part_size'} = $size if $size != 0;
        delete $get_xfer_dest_args{'disk_cache_dirname'};
    }

    return %get_xfer_dest_args;
}

sub send_port_and_get_header {
    my $self = shift;
    my ($finished_cb) = @_;

    my %subs;
    my $connected_socket;
    my $header_xfer;
    my ($xsrc, $xdst);
    my $errmsg;

    $subs{'send_port'} = make_cb(send_port => sub {
	# set up so that an incoming connection on the header socket gets sent here
	die "header_socket_connect_cb should be undef"
	    if defined $self->{'header_socket_connect_cb'};
	$self->{'header_socket_connect_cb'} = $subs{'header_socket_connect_cb'};

	# get the ip:port pairs for the data connection from the data xfer source,
	# which should be an Amanda::Xfer::Source::DirectTCPListen
	my $addrs = $self->{'xfer_source'}->get_addrs();
	$addrs = join ";", map { $_->[0] . ':' . $_->[1] } @$addrs;

	# and tell the driver which port we're listening on
	$self->{'proto'}->send(main::Protocol::PORT,
	    port => $self->{'header_socket'}->sockport(),
	    ipports => $addrs);
    });

    $subs{'header_socket_connect_cb'} = make_cb(header_socket_connect_cb => sub {
	($connected_socket) = @_; # keep the socket so Perl doesn't close it!

	# now read a header block from that socket - note that this does not specify
	# a maximum size, so this portion of Amanda at least can handle any size header
	($xsrc, $xdst) = (
	    Amanda::Xfer::Source::Fd->new($connected_socket->fileno()),
	    Amanda::Xfer::Dest::Buffer->new(0));
	$header_xfer = Amanda::Xfer->new([$xsrc, $xdst]);
	$header_xfer->start($subs{'header_xfer_xmsg_cb'});
    });

    $subs{'header_xfer_xmsg_cb'} = make_cb(header_xfer_xmsg_cb => sub {
	my ($src, $xmsg, $xfer) = @_;
	if ($xmsg->{'type'} == $XMSG_INFO) {
	    info($xmsg->{'message'});
	} elsif ($xmsg->{'type'} == $XMSG_ERROR) {
	    $errmsg = $xmsg->{'messsage'};
	} elsif ($xmsg->{'type'} == $XMSG_DONE) {
	    if ($errmsg) {
		$finished_cb->($errmsg);
	    } else {
		$subs{'got_header'}->();
	    }
	}
    });

    $subs{'got_header'} = make_cb(got_header => sub {
	my $hdr_buf = $xdst->get();

	# close stuff up
	$header_xfer = $xsrc = $xdst = undef;
	$connected_socket->close();

	# parse the header, finally!
	$self->{'header'} = Amanda::Header->from_string($hdr_buf);

	$finished_cb->(undef);
    });

    $subs{'send_port'}->();
}

# do the work of starting a new xfer; this contains the code common to
# msg_PORT_WRITE and msg_FILE_WRITE.
sub setup_dump {
    my $self = shift;
    my ($msgtype, %params) = @_;
    my %subs;

    $self->{'handle'} = $params{'handle'};
    $self->{'hostname'} = $params{'hostname'};
    $self->{'diskname'} = $params{'diskname'};
    $self->{'datestamp'} = $params{'datestamp'};
    $self->{'level'} = $params{'level'};
    $self->{'header'} = undef; # no header yet
    $self->{'last_partnum'} = -1;

    # setting up the dump is a bit complex, due to the requirements of
    # a directtcp port_write.  This function:
    # 1. creates and starts a transfer (make_xfer)
    # 2. gets the header
    # 3. calls the scribe's start_dump method with the new header

    $subs{'make_xfer'} = make_cb(make_xfer => sub {
        $self->_assert_in_state("idle") or return;
        $self->{'state'} = 'making_xfer';

        my %get_xfer_dest_args = $self->get_splitting_config($msgtype, %params);
        my $xfer_dest = $self->{'scribe'}->get_xfer_dest(%get_xfer_dest_args);

	my $xfer_source;
	if ($msgtype eq main::Protocol::PORT_WRITE) {
	    $xfer_source = Amanda::Xfer::Source::DirectTCPListen->new();
	} else {
	    $xfer_source = Amanda::Xfer::Source::Holding->new($params{'filename'});
	}
	$self->{'xfer_source'} = $xfer_source;

        $self->{'xfer'} = Amanda::Xfer->new([$xfer_source, $xfer_dest]);
        $self->{'xfer'}->start(sub {
            $self->{'scribe'}->handle_xmsg(@_);
        });

	# we've started the xfer now, but the destination won't actually write
	# any data until we call start_dump.  And we'll need a header for that.

	$subs{'get_header'}->();
    });

    $subs{'get_header'} = make_cb(get_header => sub {
        $self->_assert_in_state("making_xfer") or return;
        $self->{'state'} = 'getting_header';

	if ($msgtype eq main::Protocol::FILE_WRITE) {
	    # getting the header is easy for FILE-WRITE..
	    my $hdr = $self->{'header'} = Amanda::Holding::get_header($params{'filename'});
	    if (!defined $hdr || $hdr->{'type'} != $Amanda::Header::F_DUMPFILE) {
		die("Could not read header from '$params{filename}'");
	    }
	    $subs{'start_dump'}->(undef);
	} else {
	    # ..but quite a bit harder for PORT-WRITE; this method will send the
	    # proper PORT command, then read the header from the dumper and parse
	    # it, placing the result in $self->{'header'}
	    $self->send_port_and_get_header($subs{'start_dump'});
	}
    });

    $subs{'start_dump'} = make_cb(start_dump => sub {
	my ($err) = @_;

        $self->_assert_in_state("getting_header") or return;
        $self->{'state'} = 'writing';

        # if $err is set, call through to dump_cb to handle the situation, treating
        # it as a device error
        if ($err) {
            return $self->dump_cb(
                result => "FAILED",
                input_errors => [],
                device_errors => [ $err ],
                size => 0,
                duration => 0.0);
        }

        # sanity check the header..
        my $hdr = $self->{'header'};
        if ($hdr->{'dumplevel'} != $params{'level'}
            or $hdr->{'name'} ne $params{'hostname'}
            or $hdr->{'disk'} ne $params{'diskname'}
	    or $hdr->{'datestamp'} ne $params{'datestamp'}) {
            die("Header of dumpfile does not match command from driver");
        }

	# start producing status
	$self->create_status_file();

	# and fix it up before writing it
        $hdr->{'totalparts'} = -1;
        $hdr->{'type'} = $Amanda::Header::F_SPLIT_DUMPFILE;

        $self->{'scribe'}->start_dump(
	    xfer => $self->{'xfer'},
            dump_header => $hdr,
            dump_cb => sub { $self->dump_cb(@_); });
    });

    $subs{'make_xfer'}->();
}

sub dump_cb {
    my $self = shift;
    my %params = @_;

    # if we need to the dumper status (to differentiate a dropped network
    # connection from a normal EOF) and have not done so yet, then send a
    # DUMPER_STATUS message and re-call this method (dump_cb) with the result.
    if ($params{'result'} eq "DONE"
	    and $self->{'doing_port_write'}
	    and !exists $params{'dumper_status'}) {
	$self->{'proto'}->set_message_cb(main::Protocol::DONE,
	    make_cb(sub { $self->dump_cb(%params, dumper_status => "DONE"); }));
	$self->{'proto'}->set_message_cb(main::Protocol::FAILED,
	    make_cb(sub { $self->dump_cb(%params, dumper_status => "FAILED"); }));
	$self->{'proto'}->send(main::Protocol::DUMPER_STATUS,
		handle => $self->{'handle'});
	return;
    }

    my ($msgtype, $logtype);
    if ($params{'result'} eq 'DONE') {
	if (!$self->{'doing_port_write'} or $params{'dumper_status'} eq "DONE") {
	    $msgtype = main::Protocol::DONE;
	    $logtype = $L_DONE;
	} else {
	    $msgtype = main::Protocol::DONE;
	    $logtype = $L_PARTIAL;
	}
    } elsif ($params{'result'} eq 'PARTIAL') {
	$msgtype = main::Protocol::PARTIAL;
	$logtype = $L_PARTIAL;
    } elsif ($params{'result'} eq 'FAILED') {
	$msgtype = main::Protocol::FAILED;
	$logtype = $L_FAIL;
    }

    if ($self->{timer}) {
	$self->{timer}->remove();
	undef $self->{timer};
	$self->{status_fh}->close();
	undef $self->{status_fh};
	unlink($self->{status_filename});
	undef $self->{status_filename};
    }

    my $stats = $self->make_stats($params{'size'}, $params{'duration'});

    # write a DONE/PARTIAL/FAIL log line
    my $have_msg = @{$params{'input_errors'}} || @{$params{'device_errors'}};
    my $msg = join("; ", @{$params{'input_errors'}}, @{$params{'device_errors'}});
    $msg = quote_string($msg);

    if ($logtype == $L_FAIL) {
	log_add($L_FAIL, sprintf("%s %s %s %s %s",
	    quote_string($self->{'hostname'}.""), # " is required for SWIG..
	    quote_string($self->{'diskname'}.""),
	    $self->{'datestamp'},
	    $self->{'level'},
	    $msg));
    } else {
	log_add($logtype, sprintf("%s %s %s %s %s %s%s",
	    quote_string($self->{'hostname'}.""), # " is required for SWIG..
	    quote_string($self->{'diskname'}.""),
	    $self->{'datestamp'},
	    $self->{'last_partnum'},
	    $self->{'level'},
	    $stats,
	    ($logtype == $L_PARTIAL and $have_msg)? " $msg" : ""));
    }

    # and send a message back to the driver
    my %msg_params = (
	handle => $self->{'handle'},
    );

    if (@{$params{'input_errors'}}) {
	$msg_params{'input'} = 'INPUT-ERROR';
	$msg_params{'inputerr'} = join("; ", @{$params{'input_errors'}});
    } else {
	$msg_params{'input'} = 'INPUT-GOOD';
	$msg_params{'inputerr'} = '';
    }

    if (@{$params{'device_errors'}}) {
	$msg_params{'taper'} = 'TAPE-ERROR';
	$msg_params{'tapererr'} = join("; ", @{$params{'device_errors'}});
    } else {
	$msg_params{'taper'} = 'TAPE-GOOD';
	$msg_params{'tapererr'} = '';
    }

    if ($msgtype ne main::Protocol::FAILED) {
	$msg_params{'stats'} = $stats;
    }

    # reset things to 'idle' before sending the message
    $self->{'xfer'} = undef;
    $self->{'xfer_source'} = undef;
    $self->{'handle'} = undef;
    $self->{'header'} = undef;
    $self->{'hostname'} = undef;
    $self->{'diskname'} = undef;
    $self->{'datestamp'} = undef;
    $self->{'level'} = undef;
    $self->{'header'} = undef;
    $self->{'state'} = 'idle';

    $self->{'proto'}->send($msgtype, %msg_params);
}


package main;

use Amanda::Util qw( :constants );
use Amanda::Config qw( :init );
use Amanda::Logfile qw( :logtype_t log_add $amanda_log_trace_log );
use Amanda::Debug;
use Getopt::Long;

Amanda::Util::setup_application("taper", "server", $CONTEXT_DAEMON);

my $config_overrides = new_config_overrides($#ARGV+1);
Getopt::Long::Configure(qw{bundling});
GetOptions(
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

if (@ARGV != 1) {
    die "USAGE: taper <config> <config-overwrites>";
}

config_init($CONFIG_INIT_EXPLICIT_NAME, $ARGV[0]);
apply_config_overrides($config_overrides);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        die "Errors processing config file";
    }
}

# our STDERR is connected to the amdump log file, so be sure to do unbuffered
# writes to that file
my $old_fh = select(STDERR);
$| = 1;
select($old_fh);

log_add($L_INFO, "taper pid $$");
Amanda::Debug::add_amanda_log_handler($amanda_log_trace_log);

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# transfer control to the main::Controller class implemented above
my $controller = main::Controller->new();
$controller->start();
Amanda::MainLoop::run();

log_add($L_INFO, "pid-done $$");
