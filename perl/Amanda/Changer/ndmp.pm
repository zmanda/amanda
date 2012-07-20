# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
#
# This library is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License version 2.1 as
# published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
#
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Amanda::Changer::ndmp;

use strict;
use warnings;
use Carp;
use base 'Amanda::Changer::robot';

use Amanda::MainLoop;
use Amanda::Config qw( :getconf );
use Amanda::Debug qw( debug warning );
use Amanda::Device qw( :constants );
use Amanda::Changer;
use Amanda::NDMP;

=head1 NAME

Amanda::Changer::ndmp -- subclass of Amanda::Changer:robot to handle NDMP-based changers

=head1 DESCRIPTION

This package controls a physical tape changer via NDMP.

See the amanda-changers(7) manpage for usage information.

=cut

# NOTES
#
# This class relies on Amanda::Changer::robot for most of its functionality,
# but overrides it to insert its own Interface class (that speaks NDMP) and to
# create NDMP devices instead of tape devices.

sub get_interface {
    my $self = shift;
    my ($device_name, $ignore_barcodes) = @_;

    my ($host, $port, $scsi_dev) = ($device_name =~ /([^:@]*)(?::(\d*))?@(.*)/);
    if (!$host) {
	return Amanda::Changer->make_error("fatal", undef,
	    message => "invalid chg-ndmp specification '$device_name'");
    }
    $port = $port? ($port+0) : 0; # 0 => default port

    $self->{'ndmp-username'} = 'ndmp';
    $self->{'ndmp-password'} = 'ndmp';
    $self->{'ndmp-auth'} = 'md5';
    $self->{'verbose'} = 0;
    for my $propname (qw(ndmp-username ndmp-password ndmp-auth verbose)) {
	if (exists $self->{'config'}->{'properties'}->{$propname}) {
	    if (@{$self->{'config'}->{'properties'}->{$propname}->{'values'}} > 1) {
		return Amanda::Changer->make_error("fatal", undef,
		    message => "only one value allowed for '$propname'");
	    }
	    $self->{$propname} = $self->{'config'}->{'properties'}->{$propname}->{'values'}->[0];
	}
    }

    # assemble the arguments to NDMPConnection's constructor, so that the interface
    # can create a connection as needed
    my $connargs = [ $host, $port,
		     $self->{'ndmp-username'}, $self->{'ndmp-password'},
		     $self->{'ndmp-auth'} ];

    return Amanda::Changer::ndmp::Interface->new($connargs, $scsi_dev, $ignore_barcodes,
						 $self->{'verbose'}),
}

sub get_device {
    my $self = shift;
    my ($device_name) = @_;

    my $device = Amanda::Changer::robot::get_device($self, $device_name);

    # set the authentication properties for the new device based on our
    # own settings, but only if they haven't been set by the user
    my ($val, $surety, $source);

    ($val, $surety, $source)= $device->property_get("ndmp-auth");
    $device->property_set("ndmp-auth", $self->{'ndmp-auth'})
	if ($source == $PROPERTY_SOURCE_DEFAULT);

    ($val, $surety, $source)= $device->property_get("ndmp-password");
    $device->property_set("ndmp-password", $self->{'ndmp-password'})
	if ($source == $PROPERTY_SOURCE_DEFAULT);

    ($val, $surety, $source)= $device->property_get("ndmp-username");
    $device->property_set("ndmp-username", $self->{'ndmp-username'})
	if ($source == $PROPERTY_SOURCE_DEFAULT);

    return $device;
}

package Amanda::Changer::ndmp::Interface;

use Carp;
use Amanda::NDMP qw( :constants );
use Amanda::Debug qw( debug warning );
use Amanda::MainLoop;

sub new {
    my $class = shift;
    my ($connargs, $scsi_dev, $ignore_barcodes, $verbose) = @_;

    return bless {
	connargs => $connargs,
	scsi_dev => $scsi_dev,
	ignore_barcodes => $ignore_barcodes,
	verbose => $verbose,

	# have we called READ ELEMENT STATUS yet?
	have_status => 0,

	# this class manages the translation of SCSI element numbers to what we
	# will call 'mtx numbers', just like mtx itself does.  Specifically,
	# drives are numbered starting at 0 and slots are numbered starting at
	# 1.  These hashes map mtx numbers to scsi element numbers, and are set
	# by status()
	drive_scsi_elem_map => {},
	slot_scsi_elem_map => {},

	# to use MOVE MEDIUM, we need a medium transport element, which is stashed
	# here
	medium_transport_element => undef,
    }, $class;
}

sub inquiry {
    my $self = shift;
    my ($inquiry_cb) = @_;

    my $conn = $self->_get_scsi_conn(\$inquiry_cb);
    return $inquiry_cb->($conn->err_msg()) if $conn->err_code();

    # send a TEST UNIT READY first
    my $res = $conn->scsi_execute_cdb(
	flags => 0,
	timeout => 1*1000,
	cdb => pack('CxxxxC', 0, 0)
    );
    if (!$res) {
	return $inquiry_cb->($conn->err_msg());
    }
    if ($res->{'status'} != 0) {
	my $sense_info = $self->_get_scsi_err($res);
	return $inquiry_cb->("TEST UNIT READY failed: $sense_info");
    }

    # now send an INQUIRY
    $res = $conn->scsi_execute_cdb(
	flags => $NDMP9_SCSI_DATA_DIR_IN,
	timeout => 5*1000,
	cdb => pack('CCCnC', 0x12, 0, 0, 96, 0),
	datain_len => 96
    );
    if (!$res) {
	return $inquiry_cb->($conn->err_msg());
    }
    if ($res->{'status'} != 0) {
	my $sense_info = $self->_get_scsi_err($res);
	return $inquiry_cb->("INQUIRY failed: $sense_info");
    }

    # check that this is a media changer
    if (ord(substr($res->{'datain'}, 0, 1)) != 8) {
	return $inquiry_cb->("not a SCSI media changer device");
    }

    # extract the data we want
    my $result = {
	'vendor id' => $self->_trim_scsi(substr($res->{'datain'}, 8, 8)),
	'product id' => $self->_trim_scsi(substr($res->{'datain'}, 16, 16)),
	'revision' => $self->_trim_scsi(substr($res->{'datain'}, 32, 4)),
	'product type' => "Medium Changer",
    };

    return $inquiry_cb->(undef, $result);
}

sub status {
    my $self = shift;
    my ($status_cb) = @_;

    # the SMC spec says we can "query" the length of the READ ELEMENT STATUS
    # result by passing an initial datain_len of 8, so that's what we do.  This
    # variable will be changed, later
    my $bufsize = 8;

    my $conn = $self->_get_scsi_conn(\$status_cb);
    return $status_cb->($conn->err_msg()) if $conn->err_code();

send_cdb:
    my $res = $conn->scsi_execute_cdb(
	flags => $NDMP9_SCSI_DATA_DIR_IN,
	timeout => 60*1000, # 60-second timeout
	cdb => pack('CCnnCCnxC',
	    0xB8, # opcode
	    0x10, # VOLTAG, all element types
	    0, # start at addr 0
	    0xffff, # and give me 65535 elements
	    2, # CURDATA=1, so the robot should use its cached state
	    $bufsize >> 16, # allocation length high byte
	    $bufsize & 0xffff, # allocation length low short
	    0), # control
	datain_len => $bufsize
    );
    if (!$res) {
	return $status_cb->($conn->err_msg());
    }
    if ($res->{'status'} != 0) {
	my $sense_info = $self->_get_scsi_err($res);
	return $status_cb->("READ ELEMENT STATUS failed: $sense_info");
    }

    # if we only got the size, then send another request
    if ($bufsize == 8) {
	my ($msb, $lsw) = unpack("Cn", substr($res->{'datain'}, 5, 3));
	$bufsize = ($msb << 16) + $lsw;
	$bufsize += 8; # add the header length
	if ($bufsize > 8) {
	    goto send_cdb;
	} else {
	    return $status_cb->("got short result from READ ELEMENT STATUS");
	}
    }

    $self->{'have_status'} = 1;

    # parse it and invoke the callback
    $status_cb->(undef, $self->_parse_read_element_status($res->{'datain'}));
}

sub load {
    my $self = shift;
    my ($slot, $drive, $finished_cb) = @_;

    return $self->_do_move_medium("load", $slot, $drive, $finished_cb);
}

sub unload {
    my $self = shift;
    my ($drive, $slot, $finished_cb) = @_;

    return $self->_do_move_medium("unload", $drive, $slot, $finished_cb);
}

sub transfer {
    my $self = shift;
    my ($slot1, $slot2, $finished_cb) = @_;

    return $self->_do_move_medium("transfer", $slot1, $slot2, $finished_cb);
}

sub _do_move_medium {
    my $self = shift;
    my ($op, $src, $dst, $finished_cb) = @_;
    my $conn;
    my $steps = define_steps
	cb_ref => \$finished_cb;

    step get_conn => sub {
	$conn = $self->_get_scsi_conn(\$finished_cb);
	return $finished_cb->($conn->err_msg()) if $conn->err_code();

	$steps->{'get_status'}->();
    };

    step get_status => sub {
	if ($self->{'have_status'}) {
	    return $steps->{'send_move_medium'}->();
	} else {
	    $self->status(sub {
		my ($err, $status) = @_;
		return $finished_cb->($err) if ($err);
		return $steps->{'send_move_medium'}->();
	    });
	}
    };

    step send_move_medium => sub {
	# figure out what $slot and $drive are in terms of elements
	my ($src_elem, $dst_elem);
	if ($op eq "load") {
	    $src_elem = $self->{'slot_scsi_elem_map'}->{$src};
	    $dst_elem = $self->{'drive_scsi_elem_map'}->{$dst};
	} elsif ($op eq "unload") {
	    $src_elem = $self->{'drive_scsi_elem_map'}->{$src};
	    $dst_elem = $self->{'slot_scsi_elem_map'}->{$dst};
	} elsif ($op eq "transfer") {
	    $src_elem = $self->{'slot_scsi_elem_map'}->{$src};
	    $dst_elem = $self->{'slot_scsi_elem_map'}->{$dst};
	}

	unless (defined $src_elem) {
	    return $finished_cb->("unknown source slot/drive '$src'");
	}

	unless (defined $dst_elem) {
	    return $finished_cb->("unknown destiation slot/drive '$dst'");
	}

	# send a MOVE MEDIUM command
	my $res = $conn->scsi_execute_cdb(
	    # mtx uses data dir "out", but ndmjob uses 0.  A NetApp filer
	    # segfaults with data dir "out", so we use 0.
	    flags => $NDMP9_SCSI_DATA_DIR_NONE,
	    dataout => '',
	    # NOTE: 0 does not mean "no timeout"; it means "fail immediately"
	    timeout => 300000,
	    cdb => pack('CxnnnxxxC',
		0xA5, # MOVE MEDIUM
		$self->{'medium_transport_elem'},
		$src_elem,
		$dst_elem,
		0) # control
	);

	$steps->{'scsi_done'}->($res);
    };

    step scsi_done => sub {
	my ($res) = @_;

	if (!$res) {
	    return $finished_cb->($conn->err_msg());
	}
	if ($res->{'status'} != 0) {
	    my $sense_info = $self->_get_scsi_err($res);
	    return $finished_cb->("MOVE MEDIUM failed: $sense_info");
	}

	return $finished_cb->(undef);
    };
}

# a selected set of errors we might see; keyed by ASC . ASCQ
my %scsi_errors = (
    '0500' => "Logical Unit Does Not Respond To Selection",
    '0600' => "No Reference Position Found",
    '2101' => "Invalid element address",
    '3003' => "Cleaning Cartridge Installed",
    '3b0d' => "Medium Destination Element Full",
    '3b0e' => "Medium Source Element Empty",
    '3b11' => "Medium Magazine Not Accessible",
    '3b12' => "Medium Magazine Removed",
    '3b13' => "Medium Magazine Inserted",
    '3b14' => "Medium Magazine Locked",
    '3b15' => "Medium Magazine Unlocked",
    '3b18' => "Element Disabled",
);

sub _get_scsi_err {
    my $self = shift;
    my ($res) = @_;

    if (($res->{'status'} & 0x3E) == 2) { # CHECK CONDITION
	my @sense_data = map { ord($_) } split //, $res->{'ext_sense'};
	my $sense_key = $sense_data[1] & 0xF;
	my $sense_code = $sense_data[2];
	my $sense_code_qualifier = $sense_data[3];
	my $ascascq = sprintf("%02x%02x", $sense_code, $sense_code_qualifier);
	my $msg = "CHECK CONDITION: ";
	if (exists $scsi_errors{$ascascq}) {
	    $msg .= $scsi_errors{$ascascq} . ' - ';
	}
	$msg .= sprintf("sense key 0x%2.2x, sense code 0x%2.2x, qualifier 0x%2.2x",
	    $sense_key, $sense_code, $sense_code_qualifier);
	return $msg;
    } else {
	return "unexepected SCSI status $res->{status}";
    }
}

## non-method utilities

sub _trim_scsi {
    my $self = shift;
    my ($val) = @_;
    $val =~ s/^[ \0]*//;
    $val =~ s/[ \0]*$//;
    return $val;
}

sub _parse_read_element_status {
    my $self = shift;
    my ($data) = @_;

    # this is based on SMC-3 section 6.11.  Not all fields are converted.  Note
    # that unpack() does not support 3-byte integers, so this extracts the msb
    # (most significant byte) and lsw (least significant word) and combines them
    # $data is consumed piecemeal throughout the following.  Constants are included
    # inline, with a comment to indicate their meaning

    my $result = { drives => {}, slots => {} };
    my $next_drive_num = 0;
    my $next_slot_num = 1;
    my %slots_by_elem; # inverse of $self->{slot_scsi_elem_map}

    # element status header
    my ($first_elem, $num_elems) = unpack("nn", substr($data, 0, 4));
    $data = substr($data, 8);

    while ($data and $num_elems) { # for each element status page
	my ($elem_type, $flags, $descrip_len, $all_descrips_len_msb,
	    $all_descrips_len_lsw) = unpack("CCnxCn", substr($data, 0, 8));
	my $all_descrips_len = ($all_descrips_len_msb << 16) + $all_descrips_len_lsw;
	my $have_pvoltag = $flags & 0x80;
	my $have_avoltag = $flags & 0x40;
	confess unless $all_descrips_len % $descrip_len == 0;
	confess unless $all_descrips_len >= $descrip_len;
	confess (length($data)) unless $all_descrips_len <= length($data);
	$data = substr($data, 8);

	while ($all_descrips_len > 0) { # for each element status descriptor
	    my $descripdata  = substr($data, 0, $descrip_len);

	    my ($elem_addr, $flags, $asc, $ascq, $flags2, $src_addr) =
		unpack("nCxCCxxxCn", substr($descripdata, 0, 12));
	    my $except_flag = $flags & 0x04;
	    my $full_flag = $flags & 0x01;
	    my $svalid_flag = $flags2 & 0x80;
	    my $invert_flag = $flags2 & 0x40;
	    my $ed_flag = $flags2 & 0x08;
	    $descripdata = substr($descripdata, 12);

	    my ($pvoltag, $avoltag);
	    if ($have_pvoltag) {
		$pvoltag = $self->_trim_scsi(substr($descripdata, 0, 32));
		$descripdata = substr($descripdata, 36);
	    }
	    if ($have_avoltag) {
		$avoltag = $self->_trim_scsi(substr($descripdata, 0, 32));
		$descripdata = substr($descripdata, 36);
	    }

	    # (there's more data here, but we don't need it, so it remains unparsed)

	    if ($elem_type == 4) { # data transfer element (drive)
		my $drive = $next_drive_num++;
		$self->{'drive_scsi_elem_map'}->{$drive} = $elem_addr;

		if ($full_flag) {
		    my $h = $result->{'drives'}->{$drive} = {};
		    $h->{'barcode'} = $pvoltag;
		    # (we'll come back to this later and convert it to orig_slot)
		    $h->{'orig_slot_elem'} = $src_addr if $svalid_flag;
		} else {
		    $result->{'drives'}->{$drive} = undef;
		}
	    } elsif ($elem_type == 2 or $elem_type == 3) { # storage or import/export
		my $slot = $next_slot_num++;
		$self->{'slot_scsi_elem_map'}->{$slot} = $elem_addr;
		$slots_by_elem{$elem_addr} = $slot;

		my $h = $result->{'slots'}->{$slot} = {};
		$h->{'empty'} = 1 if !$full_flag;
		$h->{'barcode'} = $pvoltag if $pvoltag ne '';
		$h->{'ie'} = 1 if ($elem_type == 3); # import/export elem type
	    } elsif ($elem_type == 1) { # medium transport
		if (!defined $self->{'medium_transport_elem'}) {
		    $self->{'medium_transport_elem'} = $elem_addr;
		}
	    }

	    $data = substr($data, $descrip_len);
	    $all_descrips_len -= $descrip_len;
	    $num_elems--;
	}
    }

    # clean up the orig_slots, now that we have a complete mapping of mtx
    # numbers to SCSI element numbers.
    for my $dr (values %{$result->{'drives'}}) {
	next unless defined $dr;
	if (defined $dr->{'orig_slot_elem'}) {
	    $dr->{'orig_slot'} = $slots_by_elem{$dr->{'orig_slot_elem'}};
	} else {
	    $dr->{'orig_slot'} = undef;
	}
	delete $dr->{'orig_slot_elem'};
    }

    return $result;
}

# this method is responsible for opening a new NDMPConnection and calling scsi_open,
# as well as patching the given callback to automatically close the connection on
# completion.
sub _get_scsi_conn {
    my $self = shift;
    my ($cbref) = @_;

    my $conn = Amanda::NDMP::NDMPConnection->new(@{$self->{'connargs'}});
    if ($conn->err_code()) {
	return $conn;
    }

    if (!$conn->scsi_open($self->{'scsi_dev'})) {
	return $conn;
    }

    if ($self->{'verbose'}) {
	$conn->set_verbose(1);
    }

    # patch scsi_close into the callback, so it will be executed in error and
    # success conditions
    my $orig_cb = $$cbref;
    $$cbref = sub {
	my @args = @_;

	my $result = $conn->scsi_close();
	$conn = undef;
	if (!$result) {
	    if (!$args[0]) { # only report an error if one hasn't already occurred
		my $err = Amanda::Changer->make_error("fatal", undef,
		    message => "".$conn->err_msg());
		return $orig_cb->($err);
	    }
	}

	return $orig_cb->(@args);
    };

    return $conn;
}

1;
