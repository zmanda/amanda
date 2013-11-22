# Copyright (c) 2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::JSON::Changer;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );;
use Amanda::Device qw( :constants );
use Amanda::Changer;
use Amanda::Header;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Message;
use Amanda::Recovery;
use Amanda::Recovery::Scan;
use Amanda::Storage;
use Amanda::JSON::Config;
use Amanda::JSON::Tapelist;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::JSON::Changer -- JSON interface to Amanda::Changer

=head1 INTERFACE

=over

=item Amanda::JSON::Changer::inventory

Interface to C<Amanda::Changer::inventory>
Return the inventory from the changer.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::inventory",
   "params" :{"config":"test",
   "id"     :"1"}

result:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/JSON/Changer.pm",
	      "source_line":"147",
	      "code":1100000,
	      "message":"The inventory",
	      "inventory":[{"device_status":"0",
                            "label":"DIRO-TEST-001",
                            "f_type":"1",
                            "reserved":0,
                            "state":1,
                            "slot":"1"},
                           {"device_status":"0",
                            "label":"DIRO-TEST-002",
                            "f_type":"1",
                            "reserved":0,
                            "state":1,
                            "slot":"2"}],
	    }],
   "id":"1"}

=item Amanda::JSON::Changer::load

Interface to C<Amanda::Changer::load>
Return the device status or label

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::load",
   "params" :{"config":"test",
              "slot":"1"},
   "id"     :"2"}

result:
  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/JSON/Changer.pm",
	      "source_line":"241",
	      "code":1100002,
	      "load_result":{"label":"TESTCONF-001",
			     "device_status":0,
			     "f_type":"1",
			     "datestamp":"X"},
	      "message":"load result"}],
   "id":"2"}

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/JSON/Changer.pm",
	      "source_line":"241",
	      "code":1100002,
	      "load_result":{"device_status_error":"Volume not labeled",
			     "device_status":8,
			     "device_error":"File 0 not found"},
	      "message":"load result"}],
   "id":"2"}

=item Amanda::JSON::Changer::reset

Interface to C<Amanda::Changer::reset>
Reset a changer.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::reset",
   "params" :{"config":"test"},
   "id"     :"3"}

result:
  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/JSON/Changer.pm",
	      "source_line":"307",
	      "code":1100003,
	      "message":"Changer is reset"}],
   "id":"3"}

=item Amanda::JSON::Changer::eject

Interface to C<Amanda::Changer::eject>
Eject a drive.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::eject",
   "params" :{"config":"test",
	      "drive":"0"},
   "id"     :"4"}

result:
  {"jsonrpc":"2.0",
   "result":[{"source_filename":"unknown",
	      "source_line":"0",
	      "code":3,
	      "reason":"notimpl",
	      "type":"failed",
	      "message":"'chg-disk:' does not support eject"}],
   "id":"4"}

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/JSON/Changer.pm",
	      "source_line":"403",
	      "code":1100004,
              "drive":"0",
	      "message":"Drive '0' ejected"}],
   "id":"4"}

=item Amanda::JSON::Changer::clean

Interface to C<Amanda::Changer::clean>
Clean a drive.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::clean",
   "params" :{"config":"test",
	      "drive":"0"},
   "id"     :"4"}

result:
  {"jsonrpc":"2.0",
   "result":[{"source_filename":"unknown",
	      "source_line":"0",
	      "code":3,
	      "reason":"notimpl",
	      "type":"failed",
	      "message":"'chg-robot:' does not support clean"}],
   "id":"4"}

=item Amanda::JSON::Changer::verify

Interface to C<Amanda::Changer::verify>
Verify a changer is correctly configured.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::verify",
   "params" :{"config":"test"},
   "id"     :"5"}

result:
  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer/robot.pm",
	      "source_line":"1646",
	      "code":1100006,
	      "device_name":"tape:/dev/nst0",
	      "drive":"0",
	      "message":"Drive 0 is device tape:/dev/nst0"},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer/robot.pm",,
	      "source_line":"1694",
	      "code":1100008,
	      "tape_devices":" \"0=tape:/dev/nst0\"",
	      "message":"property \"TAPE-DEVICE\" \"0=tape:/dev/nst0\""}]}
   "id":"5"}

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer/robot.pm",
	      "source_line":"1637",
	      "code":1100009,
	      "device_name":"tape:/dev/nst0",
	      "drive":"2",
	      "message":"Drive 2 is not device tape:/dev/nst0"},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer/robot.pm",
	      "source_line":"1637",
	      "code":1100009,
	      "device_name":"tape:/dev/nst1",
	      "drive":"0",
	      "message":"Drive 0 is not device tape:/dev/nst1"},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer/robot.pm",
	      "source_line":"1668",
	      "code":1100007,
	      "device_name":"tape:/dev/nst0",
	      "drive":"0",
	      "message":"Drive 0 looks to be device tape:/dev/nst0"},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer/robot.pm",
	      "source_line":"1646",
	      "code":1100006,
	      "device_name":"tape:/dev/nst1",
	      "drive":"1",
	      "message":"Drive 1 is device tape:/dev/nst1"},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer/robot.pm",
	      "source_line":"1694",
	      "code":1100008,
	      "tape_devices":" \"0=tape:/dev/nst0\" \"1=tape:/dev/nst1\"",
	      "message":"property \"TAPE-DEVICE\" \"0=tape:/dev/nst0\" \"1=tape:/dev/nst1\""}]
   "id":"5"}

=item Amanda::JSON::Changer::show

Interface to C<Amanda::Changer::show>
Show what is in the changer.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::show",
   "params" :{"config":"test"},
   "id"     :"6"}

result:
  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer.pm",
	      "source_line":"1626",
	      "code":1100010,
	      "num_slots":"2",
	      "message":"scanning all 20 slots in changer"},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer.pm",
	      "source_line":"1684",
	      "code":1100015,
	      "datestamp":"20130704091458",
	      "message":"slot   1: date 20130704091458 label {E01020L4,G03020TA}",
	      "label":"{E01020L4,G03020TA}",
	      "slot":"1",
	      "write_protected":""},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer.pm",
	      "source_line":"1684",
	      "code":1100015,
	      "datestamp":"20130704091458",
	      "message":"slot   2: date 20130704091458 label {E01001L4,G03001TA}",
	      "label":"{E01001L4,G03001TA}",
	      "slot":"2",
	      "write_protected":""}],
   "id":"6"}

=item Amanda::JSON::Changer::label

Interface to C<Amanda::Changer::label>
Load a tape by label.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::label",
   "params" :{"config":"test",
	     "label":"$label"},
   "id"     :"7"}

result:
  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/JSON/Changer.pm",
	      "source_line":"814",
	      "code":1200004,
	      "label":"test-ORG-AA-vtapes-001",
	      "device":"file:/amanda/h1/vtapes/slot1",
	      "slot":"1",
	      "message":"label 'test-ORG-AA-vtapes-001' is now loaded from slot 1 in device 'file:/amanda/h1/vtapes/slot1'"}],
   "id":"7"}

=item Amanda::JSON::Changer::update

Interface to C<Amanda::Changer::update>
Update a changer.

  {"jsonrpc":"2.0",
   "method" :"Amanda::JSON::Changer::update",
   "params" :{"config":"test"},
   "id"     :"8"}

result:
  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Changer/robot.pm",
	      "source_line":"1297",
	      "code":1100019,
	      "slot":"1",
	      "message":"scanning slot 1"},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/JSON/Changer.pm",
	      "source_line":"754",
	      "code":1100018,
	      "message":"Update completed"}],
   "id":"8"}

=back

=cut

sub inventory {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    $storage = Amanda::Storage->new();
	    if ($storage->isa("Amanda::Changer::Error")) {
		push @result_messages, $storage;
		return $steps->{'done'}->();
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		push @result_messages, $chg;
		return $steps->{'done'}->();
	    }

	    return $chg->inventory(inventory_cb => $steps->{'inventory_cb'});
	};

	step inventory_cb => sub {
	    my ($err, $inventory) = @_;

	    push @result_messages, $err if $err;

	    push @result_messages, Amanda::Changer::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 1100000,
				inventory => $inventory) if $inventory;

	    return $steps->{'done'}->();
	};

	step done => sub {
	    my $err = shift;
	    push @result_messages, $err if $err;
	    return $finished_cb->();
	};
    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub load {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    $storage = Amanda::Storage->new();
	    if ($storage->isa("Amanda::Changer::Error")) {
		push @result_messages, $storage;
		return $steps->{'done'}->();
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		push @result_messages, $chg;
		return $steps->{'done'}->();
	    }

	    my %args;
	    $args{'label'} = $params{'label'} if defined $params{'label'};
	    $args{'slot'} = $params{'slot'} if defined $params{'slot'};
	    $args{'relative_slot'} = $params{'relative_slot'} if defined $params{'relative_slot'};
	    $args{'res_cb'} = $steps->{'res_cb'};
	    return $chg->load(%args);
	};

	step res_cb => sub {
	    my ($err, $res) = @_;

	    if ($err) {
		push @result_messages, $chg;
		return $steps->{'done'}->();
	    }

	    my $dev = $res->{'device'};
	    if (!defined $dev) {
		push @result_messages, Amanda::Changer::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1100001);
		return $steps->{'done'}->();
	    }

	    my $ret;
	    $ret->{'device_status'} = $dev->status;
	    if ($dev->status != $DEVICE_STATUS_SUCCESS) {
		$ret->{'device_status_error'} = $dev->status_error;
		$ret->{'device_error'} = $dev->error;
	    } else {
		my $volume_header = $dev->volume_header;
		$ret->{'f_type'} = $volume_header->{'type'};
		if ($ret->{'f_type'} == $Amanda::Header::F_TAPESTART) {
		    $ret->{'datestamp'} = $volume_header->{'datestamp'};
		    $ret->{'label'} = $volume_header->{'name'};
		}
	    }
	    push @result_messages, Amanda::Changer::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1100002,
					load_result => $ret);

	    return $res->release(finished_cb => $steps->{'done'});
	};

	step done => sub {
	    my $err = shift;
	    push @result_messages, $err if $err;
	    return $finished_cb->();
	};

    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub reset {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    $storage = Amanda::Storage->new();
	    if ($storage->isa("Amanda::Changer::Error")) {
		push @result_messages, $storage;
		return $steps->{'done'}->();
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		push @result_messages, $chg;
		return $steps->{'done'}->();
	    }

	    return $chg->reset(finished_cb => $steps->{'reset_cb'});
	};

	step reset_cb => sub {
	    my ($err, $res) = @_;

	    if ($err) {
		push @result_messages, $chg;
	    } else {
		push @result_messages, Amanda::Changer::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1100003);
	    }
	    return $steps->{'done'}->();
	};

	step done => sub {
	    my $err = shift;
	    push @result_messages, $err if $err;
	    return $finished_cb->();
	};

    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub eject {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    $storage = Amanda::Storage->new();
	    if ($storage->isa("Amanda::Changer::Error")) {
		push @result_messages, $storage;
		return $steps->{'done'}->();
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		push @result_messages, $chg;
		return $steps->{'done'}->();
	    }

	    return $chg->eject(drive => $params{'drive'},
			       finished_cb => $steps->{'eject_cb'});
	};

	step eject_cb => sub {
	    my ($err) = @_;

	    if ($err) {
		push @result_messages, $err;
	    } else {
		push @result_messages, Amanda::Changer::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1100004,
					drive  => $params{'drive'});
	    }
	    return $steps->{'done'}->();
	};

	step done => sub {
	    my $err = shift;
	    push @result_messages, $err if $err;
	    return $finished_cb->();
	};

    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub clean {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    $storage = Amanda::Storage->new();
	    if ($storage->isa("Amanda::Changer::Error")) {
		push @result_messages, $storage;
		return $steps->{'done'}->();
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		push @result_messages, $chg;
		return $steps->{'done'}->();
	    }

	    return $chg->clean(drive => $params{'drive'},
			       finished_cb => $steps->{'clean_cb'});
	};

	step clean_cb => sub {
	    my ($err) = @_;

	    if ($err) {
		push @result_messages, $err;
	    } else {
		push @result_messages, Amanda::Changer::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1100005,
					drive  => $params{'drive'});
	    }
	    return $steps->{'done'}->();
	};

	step done => sub {
	    my $err = shift;
	    push @result_messages, $err if $err;
	    $finished_cb->();
	};

    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub verify {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    $storage = Amanda::Storage->new();
	    if ($storage->isa("Amanda::Changer::Error")) {
		push @result_messages, $storage;
		return $steps->{'done'}->();
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		push @result_messages, $chg;
		return $steps->{'done'}->();
	    }

	    return $chg->verify(finished_cb => $steps->{'verify_cb'});
	};

	step verify_cb => sub {
	    my ($err, @results) = @_;

	    if ($err) {
		push @result_messages, $err;
	    } else {
		push @result_messages, @results;
	    }
	    return $steps->{'done'}->();
	};

	step done => sub {
	    my $err = shift;
	    push @result_messages, $err if $err;
	    return $finished_cb->();
	};

    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub show {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    $storage = Amanda::Storage->new();
	    if ($storage->isa("Amanda::Changer::Error")) {
		push @result_messages, $storage;
		return $steps->{'done'}->();
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		push @result_messages, $chg;
		return $steps->{'done'}->();
	    }

	    $chg->show(slots => $params{'slots'},
		       user_msg => $user_msg,
		       finished_cb => $steps->{'done'});
	};

	step done => sub {
	    my $err = shift;
	    push @result_messages, $err if $err;
	    return $finished_cb->();
	};

    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub label {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;
	my $scan;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $scan->quit() if defined $scan;
			      $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    $storage = Amanda::Storage->new();
	    if ($storage->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($storage);
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($chg);
	    }

	    $scan = Amanda::Recovery::Scan->new(chg => $chg);
	    if ($scan->isa("Amanda::Changer::Error")) {
		push @result_messages, $scan;
		return $steps->{'done'}->();
	    }
	    $scan->find_volume(label => $params{'label'},
			       res_cb => $steps->{'done_load'},
			       user_msg => $user_msg);
	};

	step done_load => sub {
	    my ($err, $res) = @_;

	    return $steps->{'done'}->($err) if $err;

	    my $gotslot = $res->{'this_slot'};
	    my $devname = $res->{'device'}->device_name;

	    push @result_messages, Amanda::Recovery::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code   => 1200004,
				slot   => $res->{'this_slot'},
				label  => $params{'label'},
				device => $res->{'device'}->device_name);
	    $res->release(finished_cb => $steps->{'done'});
	};

	step done => sub {
	    my $err = shift;
	    push @result_messages, $err if $err;
	    return $finished_cb->();
	};

    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub update {
    my %params = @_;
    my @result_messages = Amanda::JSON::Config::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    $storage = Amanda::Storage->new();
	    if ($storage->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($storage);
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($chg);
	    }

	    return $chg->update(user_msg_fn => $user_msg,
				finished_cb => $steps->{'done'});
	};

	step done => sub {
	    my $err = shift;
	    if ($err) {
		push @result_messages, $err;
	    } else {
		push @result_messages, Amanda::Changer::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code   => 1100018);
	    }
	    return $finished_cb->();
	};

    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

1;
