# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

=head1 NAME

Amanda::Chunker::Controller

=head1 DESCRIPTION

This package is a component of the Amanda chunker, and is not intended for use
by other scripts or applications.

The controller write a dumpfile to holding disk
The controller use an L<Amanda::Chunker::Scribe> object to execute the request.

=cut

use strict;
use warnings;
use Carp;

package Amanda::Chunker::Controller;

use POSIX qw( :errno_h );
use Amanda::Changer;
use Amanda::Config qw( :getconf config_dir_relative );
use Amanda::Debug qw( :logging );
use Amanda::Header;
use Amanda::Holding;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::MainLoop;
use Amanda::Chunker::Protocol;
use Amanda::Chunker::Scribe;
use Amanda::Logfile qw( :logtype_t log_add make_chunker_stats );
use Amanda::Xfer qw( :constants );
use Amanda::Util qw( quote_string );
use Amanda::Tapelist;
use File::Temp;
use Carp;

use base qw( Amanda::Chunker::Scribe::Feedback );

sub new {
    my $class           = shift;

    my $self = bless {
	state       => "init",
	timestamp   => undef,

	# filled in when a write starts:
	xfer => undef,
	xfer_source => undef,
	xfer_dest => undef,
	handle => undef,
	hostname => undef,
	diskname => undef,
	datestamp => undef,
	level => undef,
	header => undef,
	cancelled => undef,
	input_errors => [],

	# filled in after the header is available
	header => undef,

	# filled in when a new tape is started:
	label => undef
    }, $class;

    return $self;
}

sub start {
    my $self = shift;

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
        $self->{'proto'}->send(Amanda::Chunker::Protocol::BAD_COMMAND,
            message => $msg);
    });
    $self->{'proto'} = Amanda::Chunker::Protocol->new(
        rx_fh => *STDIN,
        tx_fh => *STDOUT,
        message_cb => $message_cb,
        message_obj => $self,
        debug => $Amanda::Config::debug_chunker?'chunker/driver':'',
    );
}

sub msg_QUIT {
    my $self = shift;
    my ($msgtype, %params) = @_;

    return if $self->{'quitting'};
    $self->{'quitting'} = 1;

    $self->{'state'} = 'quit';

    my $finished_cb = make_cb(finished_cb => sub {
        my $err = shift;
        if ($err) {
            Amanda::Debug::debug("Quit error: $err");
        }
        Amanda::MainLoop::quit();
    });
    $self->quit(finished_cb => $finished_cb);
}

# called when the scribe is fully started up and ready to go
sub msg_START {
    my $self = shift;
    my ($msgtype, %params) = @_;

    $self->{'timestamp'} = $params{'timestamp'};
    $self->{'state'} = 'idle';
    $self->{'max_memory'} = 32 * 32768;
    $self->{'scribe'} = Amanda::Chunker::Scribe->new(
	feedback => $self,
	debug => $Amanda::Config::debug_chunker);

    $self->{'scribe'}->start(write_timestamp => $self->{'timestamp'});
}


sub msg_PORT_WRITE {
    my $self = shift;
    my ($msgtype, %params) = @_;

    $self->_assert_in_state("idle") or return;

    $self->{'doing_port_write'} = 1 if !defined $self->{'doing_port_write'};
    # This function:
    # 1. creates and starts a transfer (make_xfer)
    # 2. gets the header
    # 3. calls the scribe's start_dump method with the new header

    #floor to 32k and convert in bytes (* 1024)
    $params{'use_bytes'} = int($params{'use_bytes'}/32) * 32 * 1024;
    $params{'chunk_size'} = int($params{'chunk_size'}/32) * 32 * 1024;

    my $dump_cb = sub { $self->dump_cb(@_); };

    my $steps = define_steps
        cb_ref => \$dump_cb;

    step setup => sub {
        $self->{'handle'} = $params{'handle'};
        $self->{'filename'} = $params{'filename'};
        $self->{'hostname'} = $params{'hostname'};
        $self->{'features'} = $params{'features'};
        $self->{'diskname'} = $params{'diskname'};
        $self->{'level'} = $params{'level'};
        $self->{'datestamp'} = $params{'datestamp'};
        $self->{'chunk_size'} = $params{'chunk_size'};
        $self->{'progname'} = $params{'progname'};
        $self->{'use_bytes'} = $params{'use_bytes'};
        $self->{'options'} = $params{'options'};
        $self->{'header'} = undef; # no header yet
        $self->{'cancelled'} = undef;
        $self->{'orig_kb'} = undef;
        $self->{'input_errors'} = [];
	$steps->{'make_xfer'}->();
    };

    step make_xfer => sub {
	$self->_assert_in_state("idle") or return;
	$self->{'state'} = 'making_xfer';
	$self->{'xfer_dest'} = $self->{'scribe'}->get_xfer_dest(
				max_memory => $self->{'max_memory'});

	if ($self->{'doing_port_write'}) {
	    $self->{'xfer_source'} = Amanda::Xfer::Source::DirectTCPListen->new();
	} else {
	    $self->{'xfer_source'} = Amanda::Xfer::Source::ShmRing->new();
	}

	$self->{'xfer'} = Amanda::Xfer->new([$self->{'xfer_source'},
					     $self->{'xfer_dest'}]);
	$self->{'xfer'}->start(sub {
            my ($src, $msg, $xfer) = @_;

	    if ($msg->{'type'} == $XMSG_CRC) {
		if ($msg->{'elt'} == $self->{'xfer_source'}) {
		    $self->{'source_server_crc'} = $msg->{'crc'}.":".$msg->{'size'};
		} elsif ($msg->{'elt'} == $self->{'xfer_dest'}) {
		    $self->{'server_crc'} = $msg->{'crc'}.":".$msg->{'size'};
		} else {
		}
	    } else {
		$self->{'scribe'}->handle_xmsg($src, $msg, $xfer);
	    }

            # if this is an error message that's not from the scribe's element, then
            # we'll need to keep track of it ourselves
            if ($msg->{'type'} == $XMSG_ERROR and $msg->{'elt'} != $self->{'xfer_dest'}) {
                push @{$self->{'input_errors'}}, $msg->{'message'};
            }

	});

        # we've started the xfer now, but the destination won't actually write
        # any data until we call start_dump.  And we'll need a header for that.

        $steps->{'get_header'}->();
    };

    step get_header => sub {
        $self->_assert_in_state("making_xfer") or return;
        $self->{'state'} = 'getting_header';

        # it, placing the result in $self->{'header'}
        $self->send_port_and_get_header($steps->{'start_dump'});
    };

    step start_dump => sub {
        my ($err) = @_;

        $self->_assert_in_state("getting_header") or return;
        $self->{'state'} = 'writing';

        # if $err is set, cancel the dump, treating it as a input error
        if ($err) {
            push @{$self->{'input_errors'}}, $err;
            return $self->{'scribe'}->cancel_dump(
                xfer => $self->{'xfer'},
                #dump_cb => sub { $self->dump_cb(@_); });
                dump_cb => $dump_cb);
        }

        # sanity check the header..
        my $hdr = $self->{'header'};
        if ($hdr->{'dumplevel'} != $params{'level'}
            or $hdr->{'name'} ne $params{'hostname'}
            or $hdr->{'disk'} ne $params{'diskname'}
            or $hdr->{'datestamp'} ne $params{'datestamp'}) {
            confess("Header of dumpfile does not match command from driver $hdr->{'dumplevel'}:$params{'level'}     $hdr->{'name'}:$params{'hostname'}     $hdr->{'disk'}:$params{'diskname'}     $hdr->{'datestamp'}:$params{'datestamp'}");
        }

        # and fix it up before writing it
        $hdr->{'totalparts'} = -1;

        $self->{'scribe'}->start_dump(
            xfer => $self->{'xfer'},
            dump_header => $hdr,
	    filename => $self->{'filename'},
	    use_bytes => $self->{'use_bytes'},
	    chunk_size => $self->{'chunk_size'},
            dump_cb => $dump_cb);
    };
}

sub msg_SHM_WRITE {
    my $self = shift;
    $self->{'doing_port_write'} = 0;
    return $self->msg_PORT_WRITE(@_);
}

sub msg_CONTINUE {
    my $self = shift;
    my ($msgtype, %params) = @_;

    #floor to 32k and convert in bytes (* 1024)
    $params{'use_bytes'} = int($params{'use_bytes'}/32) * 32 * 1024;
    $params{'chunk_size'} = int($params{'chunk_size'}/32) * 32 * 1024;

    $self->{'scribe'}->continue_chunk(%params);
}

sub msg_DONE {
    my $self = shift;
    my ($msgtype, %params) = @_;

    $self->_assert_in_state("dumper-status") or return;

    $self->{'dumper_status'} = "DONE";
    $self->{'orig_kb'} = $params{'orig_kb'};
    $self->{'client_crc'} = $params{'client_crc'};
    if (defined $self->{'result'}) {
	$self->result_cb(undef);
    }
}

sub msg_PARTIAL {
    my $self = shift;
    my ($msgtype, %params) = @_;

    $self->_assert_in_state("dumper-status") or return;

    $self->{'dumper_status'} = "PARTIAL";
    $self->{'orig_kb'} = $params{'orig_kb'};
    if (defined $self->{'result'}) {
	$self->result_cb(undef);
    }
}

sub msg_FAILED {
    my $self = shift;
    my ($msgtype, %params) = @_;

    $self->_assert_in_state("dumper-status") or return;

    $self->{'dumper_status'} = "FAILED";
    if (defined $self->{'result'}) {
	$self->result_cb(undef);
    }
}

sub msg_ABORT {
    my $self = shift;
    my ($msgtype, %params) = @_;

#    $self->_assert_in_state("writing") or return;
    if ($self->{'cancelled'} || !$self->{'handle'}) {
	return;
    }

    my $quit_cb = sub {
	$self->{'proto'}->send(Amanda::Chunker::Protocol::ABORT_FINISHED,
	    handle => $self->{'handle'});
	my $mesg;
	if ($self->{'holding_error'}) {
	    $mesg = "[$self->{'holding_error'}]";
	} else {
	    $mesg =  "[$self->{'handle'}]";
	}
	log_add($L_FAIL, sprintf("%s %s %s %s %s",
	    quote_string($self->{'hostname'}.""), # " is required for SWIG..
	    quote_string($self->{'diskname'}.""),
	    $self->{'datestamp'},
	    $self->{'level'},
	    $mesg));
    };

    $self->{'cancelled'} = 1;
    $self->{'scribe'}->quit(finished_cb => $quit_cb);
}

sub result_cb {
    my $self = shift;
    my %params = %{$self->{'dump_params'}};
    my $msgtype;
    my $logtype;
    my $msg;
    my $server_crc;

    if ($self->{'cancelled'}) {
	goto cleanup;
    }

    if (!defined $self->{'source_server_crc'}) {
	$self->{'source_server_crc'} = '00000000:0';
    }
    if (!defined $self->{'server_crc'}) {
	$self->{'server_crc'} = '00000000:0';
    }
    if (defined $self->{'client_crc'} && $self->{'client_crc'} !~ /^00000000:/ &&
	$self->{'source_server_crc'} ne '00000000:0' &&
	$self->{'client_crc'} ne $self->{'source_server_crc'}) {
	if ($params{'result'} ne 'FAILED') {
	    $params{'result'} = 'FAILED';
	    push @{$self->{'input_errors'}}, "client crc ($self->{'client_crc'}) differ from server crc ($self->{'source_server_crc'})";
	}
    }
    if ($self->{'source_server_crc'} ne '00000000:0' &&
	$self->{'source_server_crc'} ne $self->{'server_crc'}) {
	if ($params{'result'} ne 'FAILED') {
	    $params{'result'} = 'FAILED';
	    push @{$self->{'input_errors'}}, "source server crc ($self->{'source_server_crc'}) differ from server crc ($self->{'server_crc'})";
	}
    }

    if ($params{'result'} eq 'DONE' and !@{$self->{'input_errors'}}) {
	if ($self->{'dumper_status'} eq "DONE") {
	    $msgtype = Amanda::Chunker::Protocol::DONE;
	    $logtype = $L_SUCCESS;
	} elsif ($params{'data_size'} > 0) {
	    $msgtype = Amanda::Chunker::Protocol::PARTIAL;
	    $logtype = $L_PARTIAL;
	} else {
	    $msgtype = Amanda::Chunker::Protocol::FAILED;
	    $logtype = $L_FAIL;
	    $msg = "[dumper returned FAILED]";
	}
    } elsif ($params{'result'} eq 'PARTIAL') {
	$msgtype = Amanda::Chunker::Protocol::PARTIAL;
	$logtype = $L_PARTIAL;
    } else {
	$msgtype = Amanda::Chunker::Protocol::FAILED;
	$logtype = $L_FAIL;
	if (@{$self->{'input_errors'}}) {
	    $msg = join '; ', @{$self->{'input_errors'}};
	} else {
	    $msg = "chunker failed";
	}
    }

    if ($self->{timer}) {
	$self->{timer}->remove();
	undef $self->{timer};
    }

    # note that we use total_duration here, which is the total time between
    # start_dump and dump_cb.
    my $stats = make_chunker_stats($params{'data_size'}, $params{'total_duration'});

    # write a DONE/PARTIAL/FAIL log line
    if ($logtype == $L_FAIL) {
	log_add($L_FAIL, sprintf("%s %s %s %s %s",
	    quote_string($self->{'hostname'}.""), # " is required for SWIG..
	    quote_string($self->{'diskname'}.""),
	    $self->{'datestamp'},
	    $self->{'level'},
	    $msg));
    } elsif ($logtype == $L_SUCCESS) {
	log_add($logtype, sprintf("%s %s %s %s %s %s",
	    quote_string($self->{'hostname'}.""), # " is required for SWIG..
	    quote_string($self->{'diskname'}.""),
	    $self->{'datestamp'},
	    $self->{'level'},
	    $self->{'server_crc'},
	    $stats));
    } else { # L_PARTIAL
	log_add($logtype, sprintf("%s %s %s %s %s %s",
	    quote_string($self->{'hostname'}.""), # " is required for SWIG..
	    quote_string($self->{'diskname'}.""),
	    $self->{'datestamp'},
	    $self->{'level'},
	    $self->{'server_crc'},
	    $stats));
    }

    # and send a message back to the driver
    my %msg_params = (
	handle => $self->{'handle'},
    );

    if ($msgtype eq Amanda::Chunker::Protocol::FAILED) {
	$msg_params{'msg'} = $msg;
    }

    if ($msgtype eq Amanda::Chunker::Protocol::DONE ||
	$msgtype eq Amanda::Chunker::Protocol::PARTIAL) {
	$msg_params{'size'} = ($params{'data_size'}+0) / 1024;
	$msg_params{'server_crc'} = $self->{'server_crc'};
	$msg_params{'stats'} = $stats;
    }

    $self->{'proto'}->send($msgtype, %msg_params);

cleanup:
    # reset things to 'idle' before sending the message
    $self->{'xfer'} = undef;
    $self->{'xfer_source'} = undef;
    $self->{'xfer_dest'} = undef;
    $self->{'handle'} = undef;
    $self->{'header'} = undef;
    $self->{'hostname'} = undef;
    $self->{'diskname'} = undef;
    $self->{'datestamp'} = undef;
    $self->{'level'} = undef;
    $self->{'header'} = undef;
    $self->{'state'} = 'idle';
    $self->{'cancelled'} = 1;
    delete $self->{'result'};
    delete $self->{'dumper_status'};
    delete $self->{'dump_params'};
}


##
# Scribe feedback

sub request_more_disk {
    my $self = shift;

    $self->{'proto'}->send(Amanda::Chunker::Protocol::RQ_MORE_DISK,
	handle => $self->{'handle'});
}

sub notify_no_room {
    my $self = shift;
    my $use_bytes = shift;
    my $mesg = shift;

    $self->{'holding_error'} = $mesg;
    if ($use_bytes > 0) {
	$self->{'proto'}->send(Amanda::Chunker::Protocol::NO_ROOM,
	    handle => $self->{'handle'},
	    use    => $use_bytes/1024);
    }
}

sub scribe_notif_log_info {
    my $self = shift;
    my %params = @_;

    log_add($L_INFO, $params{'message'});
}

sub scribe_notif_done {
}

##
# Utilities

sub _assert_in_state {
    my $self = shift;
    my ($state) = @_;
    if ($self->{'state'} eq $state) {
	return 1;
    }

    $self->{'proto'}->send(Amanda::Chunker::Protocol::BAD_COMMAND,
	message => "command not appropriate in state '$self->{state}' : '$state'");
    return 0;
}

sub create_status_file {
    my $self = shift;

    # create temporary file
    ($self->{status_fh}, $self->{status_filename}) =
	File::Temp::tempfile("chunker_status_file_XXXXXX",
				DIR => $Amanda::Paths::AMANDA_TMPDIR,
				UNLINK => 1);

    # tell amstatus about it by writing it to the dump log
    my $qdisk = Amanda::Util::quote_string($self->{'diskname'});
    my $qhost = Amanda::Util::quote_string($self->{'hostname'});
    print STDERR "chunker: status file $qhost $qdisk:" .
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


sub dump_cb {
    my $self = shift;
    my %params = @_;

    $self->{'dump_params'} = \%params;
    $self->{'result'} = $params{'result'};

    if ($self->{'cancelled'}) {
	return $self->result_cb();
    }

    # if we need to the dumper status (to differentiate a dropped network
    # connection from a normal EOF) and have not done so yet, then send a
    # DUMPER_STATUS message and re-call this method (dump_cb) with the result.
    if ($params{'result'} eq "DONE"
	    and !exists $self->{'dumper_status'}) {
	$self->{'proto'}->send(Amanda::Chunker::Protocol::DUMPER_STATUS,
		handle => $self->{'handle'});
	$self->{'state'} = 'dumper-status';
    } else {
	$self->result_cb();
    }
}

sub quit {
    my $self = shift;
    my %params = @_;
    my @errors = ();

    my $steps = define_steps
	cb_ref => \$params{'finished_cb'};

    step quit_scribe => sub {
	if (defined $self->{'scribe'}) {
	    $self->{'scribe'}->quit(finished_cb => $steps->{'stop_proto'});
	} else {
	    $steps->{'stop_proto'}->();
	}
    };

    step stop_proto => sub {
        $self->{'proto'}->stop(finished_cb => sub {
            my ($err) = @_;
            push @errors, $err if ($err);

            $steps->{'done'}->();
        });
    };

    step done => sub {
        if (@errors) {
            $params{'finished_cb'}->(join("; ", @errors));
        } else {
            $params{'finished_cb'}->();
        }
    };
}

sub send_port_and_get_header {
    my $self = shift;
    my ($finished_cb) = @_;

    my $header_xfer;
    my ($xsrc, $xdst);
    my $errmsg;

    my $steps = define_steps
        cb_ref => \$finished_cb;

    step start => sub {
	if ($self->{'doing_port_write'}) {
	    $steps->{'send_port'}->();
	} else {
	    $steps->{'send_shm_name'}->();
	}
    };

    step send_shm_name => sub {
        # get the shm_name
        my $shm_name = $self->{'xfer_source'}->get_shm_name();

        # and set up an xfer for the header, too, using DirectTCP as an easy
        # way to implement a listen/accept/read process.  Note that this does
        # not enforce a maximum size, so this portion of Amanda at least can
        # handle any size header
        ($xsrc, $xdst) = (
            Amanda::Xfer::Source::DirectTCPListen->new(),
            Amanda::Xfer::Dest::Buffer->new(0));
        $header_xfer = Amanda::Xfer->new([$xsrc, $xdst]);
        $header_xfer->start($steps->{'header_xfer_xmsg_cb'});

        my $header_addrs = $xsrc->get_addrs();
        my $header_port = $header_addrs->[0][1];

        # and tell the driver which ports we're listening on
        $self->{'proto'}->send(Amanda::Chunker::Protocol::SHM_NAME,
            worker_name => $self->{'worker_name'},
            handle => $self->{'handle'},
            port => $header_port,
            shm_name => $shm_name);
    };

    step send_port => sub {
        # get the ip:port pairs for the data connection from the data xfer source,
        # which should be an Amanda::Xfer::Source::DirectTCPListen
        my $data_addrs = $self->{'xfer_source'}->get_addrs();
        $data_addrs = join ";", map { $_->[0] . ':' . $_->[1] } @$data_addrs;

        # and set up an xfer for the header, too, using DirectTCP as an easy
        # way to implement a listen/accept/read process.  Note that this does
        # not enforce a maximum size, so this portion of Amanda at least can
        # handle any size header
        ($xsrc, $xdst) = (
            Amanda::Xfer::Source::DirectTCPListen->new(),
            Amanda::Xfer::Dest::Buffer->new(0));
        $header_xfer = Amanda::Xfer->new([$xsrc, $xdst]);
        $header_xfer->start($steps->{'header_xfer_xmsg_cb'});

        my $header_addrs = $xsrc->get_addrs();
        my $header_port = $header_addrs->[0][1];

        # and tell the driver which ports we're listening on
        $self->{'proto'}->send(Amanda::Chunker::Protocol::PORT,
            worker_name => $self->{'worker_name'},
            handle => $self->{'handle'},
            port => $header_port,
            ipport => $data_addrs);
    };

    step header_xfer_xmsg_cb => sub {
        my ($src, $xmsg, $xfer) = @_;
        if ($xmsg->{'type'} == $XMSG_INFO) {
            info($xmsg->{'message'});
        } elsif ($xmsg->{'type'} == $XMSG_ERROR) {
            $errmsg = $xmsg->{'message'};
        } elsif ($xmsg->{'type'} == $XMSG_DONE) {
            if ($errmsg) {
                $finished_cb->("Get header: $errmsg");
            } else {
                $steps->{'got_header'}->();
            }
        } else {
	    Amanda::Debug::debug("got $xmsg->{'type'} message from $xmsg->{'elt'} ");
	}
    };

    step got_header => sub {
        my $hdr_buf = $xdst->get();

        # close stuff up
        $header_xfer = $xsrc = $xdst = undef;

        if (!defined $hdr_buf) {
            return $finished_cb->("Got empty header");
        }

        # parse the header, finally!
        $self->{'header'} = Amanda::Header->from_string($hdr_buf);

        $finished_cb->(undef);
    };
}

1;
