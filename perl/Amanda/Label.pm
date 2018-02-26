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

package Amanda::Label::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 1000000) {
	return "volume '$self->{'pool'}:$self->{'label'}': Can't assign because it is in the '$self->{'config'}' config";
    } elsif ($self->{'code'} == 1000001) {
	return "volume '$self->{'pool'}:$self->{'label'}': Can't assign meta-label without force, old meta-label is '$self->{'meta'}'";
    } elsif ($self->{'code'} == 1000002) {
	return "volume '$self->{'pool'}:$self->{'label'}': Can't assign barcode without force, old barcode is '$self->{'barcode'}'";
    } elsif ($self->{'code'} == 1000003) {
	return  "volume '$self->{'pool'}:$self->{'label'}': Can't assign pool without force, old pool is '$self->{'pool'}'";
    } elsif ($self->{'code'} == 1000004) {
	return "volume '$self->{'pool'}:$self->{'label'}': Can't assign storage without force, old storage is '$self->{'storage'}'";
    } elsif ($self->{'code'} == 1000005) {
	return "$self->{'label'}: Can't assign storage because it is a new labelled tape.";
    } elsif ($self->{'code'} == 1000006) {
	return "Setting volume '$self->{'pool'}:$self->{'label'}'";
    } elsif ($self->{'code'} == 1000007) {
	return "label '$self->{'pool'}:$self->{'label'}' already correctly set.";
    } elsif ($self->{'code'} == 1000008) {
	return "Reading label...";
    } elsif ($self->{'code'} == 1000009) {
	return "Found an empty tape.";
    } elsif ($self->{'code'} == 1000010) {
	return "Found a non-Amanda tape.";
    } elsif ($self->{'code'} == 1000011) {
	return "Error reading volume label: $self->{'dev_error'}.";
    } elsif ($self->{'code'} == 1000012) {
	return "Error reading volume label: $self->{'dev_error'}.";
    } elsif ($self->{'code'} == 1000013) {
	return "Label '$self->{'label'}' doesn't match the labelstr '$self->{'labelstr'}->{'template'}'.";
    } elsif ($self->{'code'} == 1000014) {
	return "Found label '$self->{'label'}' but it is from config '$self->{'config'}'";
    } elsif ($self->{'code'} == 1000015) {
	return "Found label '$self->{'label'}' but it is from tape pool '$self->{'oldpool'}'.";
    } elsif ($self->{'code'} == 1000016) {
	return "Found label '$self->{'label'}' but it doesn't match the labelstr '$self->{'labelstr'}->{'template'}'.";
    } elsif ($self->{'code'} == 1000017) {
	return "Volume with label '$self->{'label'}' is active and contains data from this configuration.";
    } elsif ($self->{'code'} == 1000018) {
	return "Consider using 'amrmtape' to remove volume '$self->{'label'}' from the catalog.";
    } elsif ($self->{'code'} == 1000019) {
	return "Found volume '$self->{'pool'}:$self->{'label'}' but it is not in the catalog.";
    } elsif ($self->{'code'} == 1000020) {
	return "Writing label '$self->{'label'}'...";
    } elsif ($self->{'code'} == 1000021) {
	return "Checking label...";
    } elsif ($self->{'code'} == 1000022) {
	return "Success!"
    } elsif ($self->{'code'} == 1000023) {
	return "Label '$self->{'pool'}:$self->{'label'}' already on a volume.";
    } elsif ($self->{'code'} == 1000024) {
	return "No volume with barcode '$self->{'barcode'}' available.";
    } elsif ($self->{'code'} == 1000025) {
	return "Volume in slot $self->{'slot'} have barcode '$self->{'res_barcode'}, it is not '$self->{'barcode'}'.";
    } elsif ($self->{'code'} == 1000026) {
	return "Volume in slot $self->{'slot'} have no barcode.";
    } elsif ($self->{'code'} == 1000027) {
	return "Device meta '$self->{'dev_meta'}' is not the same as the --meta argument '$self->{'meta'}'"
    } elsif ($self->{'code'} == 1000028) {
	return "Error writing label: $self->{'dev_error'}.";
    } elsif ($self->{'code'} == 1000029) {
	return "Error finishing label: $self->{'dev_error'}.";
    } elsif ($self->{'code'} == 1000030) {
	return "Checking the tape label failed: $self->{'dev_error'}.";
    } elsif ($self->{'code'} == 1000031) {
	return "No label found.";
    } elsif ($self->{'code'} == 1000032) {
	return "Read back a different label: got '$self->{'got_label'}', but expected '$self->{'label'}'.";
    } elsif ($self->{'code'} == 1000033) {
	return "Read back a different timetstamp: got '$self->{'got_timestamp'}', but expected 'X'.";
    } elsif ($self->{'code'} == 1000034) {
	return "Not writing label.";
    } elsif ($self->{'code'} == 1000035) {
	return "volume '$self->{'pool'}:$self->{'label'}' not found in catalog '$self->{'catalog_name'}'.";
    } elsif ($self->{'code'} == 1000036) {
	return "Failed to copy/backup '$self->{'tapelist_filename'}' to '$self->{'backup_tapelist'}': $self->{'strerror'}.";
    } elsif ($self->{'code'} == 1000037) {
	return "Failed to execute $self->{'program'}: $self->{'strerror'} $self->{'exit_value'}.";
    } elsif ($self->{'code'} == 1000038) {
	return "Failed to open $self->{'filename'} for writing: $self->{'strerror'} $self->{'exit_value'}.";
    } elsif ($self->{'code'} == 1000039) {
	return "unexpected number of fields in \"stats\" entry for $self->{'host'}:$self->{'disk'}\n\t$self->{'line'}"
    } elsif ($self->{'code'} == 1000040) {
	return "Discarding Host: $self->{'host'}, Disk: $self->{'disk'}, Level: $self->{'level'}.";
    } elsif ($self->{'code'} == 1000041) {
	return "Error: unrecognized line of input:\n\t$self->{'line'}";
    } elsif ($self->{'code'} == 1000042) {
	return "Failed to rollback new tapelist: $self->{'strerror'} $self->{'exit_value'}.";
    } elsif ($self->{'code'} == 1000043) {
	return "'$self->{'program'}' exited with non-zero while exporting: $self->{'strerror'} $self->{'exit_value'}.";
    } elsif ($self->{'code'} == 1000044) {
	return "'$self->{'program'}' exited with non-zero while importing: $self->{'strerror'} $self->{'exit_value'}.";
    } elsif ($self->{'code'} == 1000045) {
	return "marking volume '$self->{'pool'}:$self->{'label'}' as reusable.";
    } elsif ($self->{'code'} == 1000046) {
	return "volume '$self->{'pool'}:$self->{'label'}' already reusable.";
    } elsif ($self->{'code'} == 1000047) {
	return "marking volume '$self->{'pool'}:$self->{'label'}' as not reusable.";
    } elsif ($self->{'code'} == 1000048) {
	return "volume '$self->{'pool'}:$self->{'label'}' already not reusable.";
    } elsif ($self->{'code'} == 1000049) {
	return "Erased volume with label '$self->{'label'}'.";
    } elsif ($self->{'code'} == 1000050) {
	return "Rewrote label '$self->{'label'}' to volume.";
    } elsif ($self->{'code'} == 1000051) {
	return "Can not erase '$self->{'label'} because the device doesn't support this feature";
    } elsif ($self->{'code'} == 1000052) {
	return "Removed volume '$self->{'pool'}:$self->{'label'}' from catalog '$self->{'catalog_name'}'";
    } elsif ($self->{'code'} == 1000053) {
	return "Failed to erase volume with label '$self->{'label'}'.";
    } elsif ($self->{'code'} == 1000054) {
	return "amtrmlog exited with non-zero while scrubbing logs: $self->{'errnostr'} $self->{'child_error'}.";
    } elsif ($self->{'code'} == 1000055) {
	return "amtrmidx exited with non-zero while scrubbing logs: $self->{'errnostr'} $self->{'child_error'}.";
    } elsif ($self->{'code'} == 1000056) {
	return "Failed to rewrite label '$self->{'label'}' to volume: $self->{'dev_error'}.";
    } elsif ($self->{'code'} == 1000057) {
	return "Set datestamp to \"0\" for volume '$self->{'pool'}:$self->{'label'} in catalog '$self->{'catalog_name'}'";
    } elsif ($self->{'code'} == 1000058) {
	return "Can't remove the pool of label '$self->{'label'}'.";
    } elsif ($self->{'code'} == 1000059) {
	return "Can't remove the storage of label '$self->{'label'}'.";
    } elsif ($self->{'code'} == 1000060) {
	return "Can't remove the config of label '$self->{'label'}'.";
    } elsif ($self->{'code'} == 1000061) {
	return "No label matching '$self->{'label'}' in the tapelist file";
    } elsif ($self->{'code'} == 1000062) {
	return "$self->{'dev_error'}";
    } elsif ($self->{'code'} == 1000063) {
	return "More than one volume matching label '$self->{'pool'}:$self->{'label'}' in catalog '$self->{'catalog_name'}', you must specify the pool";
    } else {
	return "no message for code $self->{'code'}";
    }
}

package Amanda::Label;

use strict;
use warnings;

use File::Basename;
use File::Copy;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants match_labelstr );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Header qw( :constants );
use Amanda::MainLoop;
use Amanda::Paths;


=head1 NAME

Amanda::Label - Amanda module to handle label on volume.

=head1 SYNOPSIS

  # create a Label class
  my $Label = Amanda::Label->new(storage  => $storage,
				 catalog  => $catalog,
				 user_msg => \&user_msg);

  $Label->assign(...);
  $Label->label(...);
  $Label->erase(...);
  $Label->reuse(...);
  $label->no_reuse(...);

=head1 user_msg callback

The C<user_msg> is a method that take an Amanda::Message as argument.
This methos is called every time a message must be sent to the caller.

=head1 Label Objects

=head2 assign

  $Label->assign(finished_cb => $cb,
		 label       => $label,
		 meta        => $meta,
		 barcode     => $barcode,
		 pool        => $pool,
		 storage     => $storage,
		 comment     => $comment,
		 force       => $force);

Assign the C<meta>, C<barcode>, C<pool> and C<storage> to the label, they can
be undef to not modify the setting. An empty string will remove the setting.

The C<finished_cb> method is called with an Amanda::Message argument.

=head2 label

  $Label->label(finished_cb => $cb,
		label       => $label,
		slot        => $slot,
		barcode     => $barcode,
		meta        => $meta,
		force       => $force);

Label a volume with the C<label>, the volume is selected  by the C<slot> or C<barcode> or the current slot.
C<meta> verifies it is the same as on the changer.

The C<finished_cb> method is called with an Amanda::Message argument.

=head2 erase

  $Label->erase(finished_cb   => $cb,
		labels        => @labels,
		erase         => $erase,
		keep_label    => $keep_label,
		external_copy => $external_copy,
		cleanup       => $cleanup,
		dry_run       => $dry_run);

Remove C<labels> from the amanda database, erase the volume is C<erase> is set,
keep the label is C<keep_label> is set,

Run amtrmlog and amtrmidx is C<cleanup> is set.

Do nothing is C<dry_run> is set.

The C<finished_cb> method is called with an Amanda::Message argument.

=head2 reuse

  $Label->reuse(finished_cb => $cb,
		labels      => @labels);

Mark all C<labels> as reuse, calling the changer set_reuse method.

The C<finished_cb> method is called with an Amanda::Message argument.

=head2 no_reuse

  $Label->no_reuse(finished_cb => $cb,
		   labels      => @labels);

Mark all C<labels> as reuse, calling the changer set_no_reuse method.

The C<finished_cb> method is called with an Amanda::Message argument.

=cut

my $amadmin = "$sbindir/amadmin";
my $amtrmidx = "$amlibexecdir/amtrmidx";
my $amtrmlog = "$amlibexecdir/amtrmlog";

sub new {
    my $class = shift @_;
    my %params = @_;

    my $self = \%params;
    bless $self, $class;

    return $self;
}

sub user_msg {
    my $self = shift;
    my $msg = shift;

    if (defined $self->{'user_msg'}) {
	if (UNIVERSAL::isa($msg, 'ARRAY')) {
	    foreach my $amsg (@{$msg}) {
		$self->{'user_msg'}->($amsg);
	    }
	} else {
	    $self->{'user_msg'}->($msg);
	}
    }
}

sub assign {
    my $self = shift;
    my %params = @_;

    my $finished_cb = $params{'finished_cb'};
    my @labels;
    @labels = @{$params{'labels'}} if defined $params{'labels'};
    push @labels, $params{'label'} if defined $params{'label'};

    my $count = 0;
    my $exit_status = 0;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {

	foreach my $label (@labels) {
	    $self->user_msg($self->{'catalog'}->volume_assign(
				$params{'force'},
                                $self->{'storage'}->{'tapepool'}, $label,
				$params{'pool'},
				$params{'barcode'},
				$params{'storage'},
				$params{'meta'},
				$params{'reuse'}));
	}

	if (defined $params{'meta'} && !defined $params{'storage'}) {
	    return $self->{'storage'}->{'chg'}->inventory(inventory_cb => $steps->{'assign_inventory'});
	}
	$steps->{'assign_reuse'}->();
    };

    step assign_inventory => sub {
	my ($err, $inv) = @_;

	if ($err) {
	    return $finished_cb->() if $err->notimpl;
	    return $finished_cb->(Amanda::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code    => 1,
					severity => $Amanda::Message::ERROR,
					message => $err));
	}

	for my $sl (@$inv) {
	    if (defined $sl->{'label'} && ($sl->{'label'} =~ /$params{'label'}/ ||
					   $sl->{'label'} eq $params{'label'})) {
		$count++;
		$self->{'storage'}->{'chg'}->set_meta_label(
					    meta => $params{'meta'},
					    slot => $sl->{'slot'},
					    finished_cb => $steps->{'assign_done'});
	    }
	}
	return $steps->{'maybe_done'}->();
    };

    step assign_done => sub {
	--$count;
    };

    step maybe_done => sub {
	return $steps->{'assign_reuse'}->() if $count == 0;
	return $steps->{'maybe_done'}->();
    };

    step assign_reuse => sub {
	if (defined $params{'reuse'}) {
	    if ($params{'reuse'} == 1) {
		return $self->{'storage'}->{'chg'}->set_reuse(
				labels => \@labels,
				finished_cb => $steps->{'reuse_done'});
	    } else {
		return $self->{'storage'}->{'chg'}->set_no_reuse(
				labels => \@labels,
				finished_cb => $steps->{'reuse_done'});
	    }
	}
	return $finished_cb->();
    };

    step reuse_done => sub {
	my $err = shift;
	if ($err and $err->notimpl) {
	    $err = undef;
	}

	$self->user_msg($err) if $err;
	$finished_cb->();
    }
}

# return undef on success, a string on error.
sub label {
    my $self = shift;
    my %params = @_;

    my $gerr;
    my $chg;
    my $dev;
    my $dev_ok;
    my $labelstr;
    my $autolabel;
    my $tapepool;
    my $storage_name;
    my $comment;
    my $finished_cb = $params{'finished_cb'};
    my $res;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {

	$chg = $self->{'storage'}->{'chg'};

	$labelstr = $self->{'storage'}->{'labelstr'};
	$autolabel = $self->{'storage'}->{'autolabel'};
	$tapepool = $self->{'storage'}->{'tapepool'};
	$storage_name = $self->{'storage'}->{'storage_name'};
	$comment  = $params{'comment'};

	if (defined($params{'label'}) && !$params{'force'}) {
	    my $volume = $self->{'catalog'}->find_volume($tapepool, $params{'label'});
	    if ($volume) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000023,
			severity => $Amanda::Message::ERROR,
			label  => $params{'label'},
			pool   => $tapepool,
			catalog_name => $self->{'catalog'}->{'catalog_name'}));
	    }
	}

	$steps->{'load'}->();
    };

    step load => sub {
	$self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000008,
			severity => $Amanda::Message::INFO));
	if ($params{'slot'}) {
	    $chg->load(slot => $params{'slot'}, mode => "write",
		       res_cb => $steps->{'loaded'});
	} elsif ($params{'barcode'}) {
	    $chg->inventory(inventory_cb => $steps->{'inventory'});
	} else {
	    $chg->load(relative_slot => "current", mode => "write",
		       res_cb => $steps->{'loaded'});
	}
    };

    step inventory => sub {
	my ($err, $inv) = @_;

	return $steps->{'releasing'}->($err) if $err;

	for my $sl (@$inv) {
	    if ($sl->{'barcode'} eq $params{'barcode'}) {
		return $chg->load(slot => $sl->{'slot'}, mode => "write",
				  res_cb => $steps->{'loaded'});
	    }
	}

	return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code     => 1000024,
					severity => $Amanda::Message::ERROR,
					barcode  => $params{'barcode'}));
    };

    step loaded => sub {
	(my $err, $res) = @_;

	return $steps->{'releasing'}->($err) if $err;

	if (defined $params{'slot'} && defined $params{'barcode'} &&
	    $params{'barcode'} ne $res->{'barcode'}) {
	    if (defined $res->{'barcode'}) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code        => 1000025,
					severity     => $Amanda::Message::ERROR,
					slot        => $params{'slot'},
					barcode     => $params{'barcode'},
					red_barcode => $res->{'barcode'}));
	    } else {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code        => 1000026,
					severity    => $Amanda::Message::ERROR,
					slot        => $params{'slot'}));
	    }
	}
	$dev = $res->{'device'};
	$dev_ok = 1;
	if ($dev->status & $DEVICE_STATUS_VOLUME_UNLABELED) {
	    if (!$dev->volume_header or $dev->volume_header->{'type'} == $F_EMPTY) {
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000009,
					severity    => $Amanda::Message::INFO));
	    } else {
		# force is required for non-Amanda tapes
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000010,
					severity    => $params{'force'}?$Amanda::Message::INFO:$Amanda::Message::ERROR));
		$dev_ok = 0 unless ($params{'force'});
	    }
	} elsif ($dev->status & $DEVICE_STATUS_VOLUME_ERROR) {
	    # it's OK to force through VOLUME_ERROR
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000011,
				severity  => $params{'force'}?$Amanda::Message::INFO:$Amanda::Message::ERROR,
				dev_error => $dev->error_or_status()));
	    $dev_ok = 0 unless ($params{'force'});
	} elsif ($dev->status != $DEVICE_STATUS_SUCCESS) {
	    # but anything else is fatal
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000012,
				severity  => $Amanda::Message::ERROR,
				dev_error => $dev->error_or_status()));
	    $dev_ok = 0;
	} else {
	    # this is a labeled Amanda tape
	    my $label = $dev->volume_label;
	    my $barcode = $res->{'barcode'};
	    my $meta = $res->{'meta'};
	    my $volumes = $self->{'catalog'}->find_volume_all($tapepool, $label);
	    my $volume = $volumes->[0];

	    if ($label && !$volume) {
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000019,
					severity  => $params{'force'}?$Amanda::Message::INFO:$Amanda::Message::ERROR,
					pool	  => $tapepool,
					label     => $label));
		$dev_ok = 0 unless ($params{'force'});
	    } elsif ($label &&
		$labelstr->{'template'} &&
		!match_labelstr($labelstr, $autolabel, $label, $barcode, $meta, $storage_name)) {
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000016,
					severity  => $Amanda::Message::ERROR,
					pool      => $tapepool,
					label     => $label,
					labelstr  => $labelstr));
		$dev_ok = 0 unless ($params{'force'});
	    } elsif ($volume) {
		if ($volume->{'config'} && $volume->{'config'} ne Amanda::Config::get_config_name()) {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000014,
					severity  => $Amanda::Message::ERROR,
					pool      => $tapepool,
					label     => $label,
					config    => $volume->{'config'}));
		    $dev_ok = 0 unless ($params{'force'});
		} elsif ($volume->{'pool'} &&
			 $volume->{'pool'} ne $tapepool) {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000015,
					severity  => $Amanda::Message::ERROR,
					pool      => $tapepool,
					label     => $label,
					oldpool   => $volume->{'pool'}));
		    $dev_ok = 0 unless ($params{'force'});
		} elsif (!$volume->{'pool'} &&
			 !match_labelstr($labelstr, $autolabel, $label, $barcode, $meta, $storage_name)) {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000016,
					severity  => $Amanda::Message::ERROR,
					pool      => $tapepool,
					label     => $label,
					labelstr  => $labelstr));
		    $dev_ok = 0 unless ($params{'force'});
		} else {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000017,
					severity  => $params{'force'}?$Amanda::Message::INFO:$Amanda::Message::ERROR,
					pool      => $tapepool,
					label     => $label));
		    if ($params{'force'}) {
			# if -f, then the user should clean things up..
			$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000018,
					severity  => $Amanda::Message::INFO,
					pool      => $tapepool,
					label     => $label));
			# note that we don't run amrmtape automatically, as it could result in data loss when
			# multiple volumes have (perhaps accidentally) the same label
		    } else {
			$dev_ok = 0;
		    }
		}
	    } else {
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000019,
					severity  => $params{'force'}?$Amanda::Message::INFO:$Amanda::Message::ERROR,
					pool      => $tapepool,
					label     => $label));
		$dev_ok = 0 unless ($params{'force'});
	    }
	}

	$res->get_meta_label(finished_cb => $steps->{'got_meta'});
    };

    step got_meta => sub {
	my ($err, $meta) = @_;

	return $steps->{'releasing'}->($err) if defined $err;

	if (defined $meta && defined $params{'meta'} && $meta ne $params{'meta'}) {
	    return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code        => 1000027,
					severity  => $Amanda::Message::ERROR,
					dev_meta    => $meta,
					meta        => $params{'meta'}));
	}
	$meta = $params{'meta'} if !defined $meta;
	($meta, my $merr) = $res->make_new_meta_label() if !defined $meta;
	if (defined $merr) {
	    return $steps->{'releasing'}->($merr);
	}
	$params{'meta'} = $meta;

	my $label = $params{'label'};
	if (!defined($label)) {
	    ($label, my $lerr) = $res->make_new_tape_label(meta => $meta);
	    if (defined $lerr) {
		return $steps->{'releasing'}->($lerr);
	    }
	}

	if ($params{'label'} && $dev_ok) {
	    my $barcode = $res->{'barcode'};
	    my $meta = $meta;

	    if (!match_labelstr($labelstr, $autolabel, $params{'label'}, $barcode, $meta, $storage_name)) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000013,
					severity  => $Amanda::Message::ERROR,
					label     => $label,
					labelstr  => $labelstr));
	    }
	}
	if ($dev_ok){
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000020,
				severity  => $Amanda::Message::INFO,
				label     => $label));

	    if (!$dev->start($ACCESS_WRITE, $label, "X")) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000028,
					severity  => $Amanda::Message::ERROR,
					dev_error => $dev->error_or_status()));
	    } elsif (!$dev->finish()) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000029,
					severity  => $Amanda::Message::ERROR,
					dev_error => $dev->error_or_status()));
	    }

	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000021,
				severity  => $Amanda::Message::INFO));
	    my $status = $dev->read_label();
	    if ($status != $DEVICE_STATUS_SUCCESS) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000030,
					severity  => $Amanda::Message::ERROR,
					dev_error => $dev->error_or_status()));
	    } elsif (!$dev->volume_label) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000031,
					severity  => $Amanda::Message::ERROR));
	    } elsif ($dev->volume_label ne $label) {
		my $got = $dev->volume_label;
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000032,
					severity  => $Amanda::Message::ERROR,
					label     => $label,
					got_label => $got));
	    } elsif ($dev->volume_time ne "X") {
		my $got = $dev->volume_time;
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000033,
					severity  => $Amanda::Message::ERROR,
					got_timestamp => $got));
	    }

	    # update the catalog
	    $self->{'catalog'}->add_volume($tapepool, $label, 0, $storage_name, $meta, $res->{'barcode'}, $dev->block_size/1024, 1, 0, 0, 0, 0);

	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000022,
				severity  => $Amanda::Message::SUCCESS));

	    # notify the changer
	    $res->set_label(label => $label, finished_cb => $steps->{'set_meta_label'});
	} else {
	    return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000034,
					severity  => $Amanda::Message::ERROR));
	}
    };

    step set_meta_label => sub {
	my ($gerr) = @_;

	if ($params{'meta'}) {
	    return $res->set_meta_label(meta => $params{'meta'},
					finished_cb => $steps->{'releasing'});
	} else {
	    return $steps->{'releasing'}->();
	}
    };

    step releasing => sub {
	my ($err) = @_;

	$self->user_msg($err) if $err;
	$gerr = $err if !$gerr;

	return $res->release(finished_cb => $steps->{'released'}) if $res;
	return $steps->{'released'}->();
    };

    step released => sub {
	my ($err) = @_;

	$res = undef;

	$err = $gerr if defined $gerr;
	if ($err) {
	    if (!$err->isa("Amanda::Message")) {
		$err = Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1,
				severity  => $Amanda::Message::ERROR,
				message   => "$err");
	    }
	    return $finished_cb->($err);
	}

	$finished_cb->();
    };

}


sub erase {
    my $self = shift;
    my %params = @_;
    my $storage;
    my $chg;
    my $res;
    my $label;
    my $pool;
    my $gerr;
    my $backup_tapelist_file;
    my $tmp_curinfo_file;

    my $finished_cb = $params{'finished_cb'};

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $storage->quit() if defined $storage;
			  $chg->quit() if defined $chg };

    step start => sub {
	$label = shift @{$params{'labels'}};

	if (!defined $label) {
	    return $steps->{'cleanup'}->();
	}

	$pool = $params{'pool'};
	if (!defined $pool && $params{'storage'}) {
	    my $st = lookup_storage($params{'storage'});
	    if ($st) {
		$pool = storage_getconf($st, $STORAGE_TAPEPOOL);
	    }
	}
	if (!defined $pool && $self->{'storage'}) {
	    my $st = lookup_storage($self->{'storage'});
	    if ($st) {
		$pool = storage_getconf($st, $STORAGE_TAPEPOOL);
	    }
	}
	my $volumes = $self->{'catalog'}->find_volume_all($pool, $label);
	if (@$volumes == 0) {
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000035,
			severity  => $Amanda::Message::ERROR,
			label  => $label,
			pool  => $pool,
			catalog_name => $self->{'catalog'}->{'catalog_name'}));
	    return $steps->{'start'}->();
	}
	if (@$volumes > 1) {
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000063,
			severity  => $Amanda::Message::ERROR,
			label  => $label,
			pool  => $pool,
			catalog_name => $self->{'catalog'}->{'catalog_name'}));
	    return $steps->{'start'}->();
	}
	$pool = $volumes->[0]->{'pool'};

	return $steps->{'erase'}->($volumes->[0]);
    };

    step erase => sub {
	my $volume = shift;

	if ($params{'erase'}) {
	    if (defined $storage and
		$storage->{'storage_name'} ne $volume->{'storage'}) {
		$storage->quit() if defined $storage; $storage = undef;
		$chg->quit() if defined $chg; $chg = undef;
	    }
	    if (!$storage) {
		$storage = Amanda::Storage->new(storage_name => $volume->{'storage'},
						catalog => $self->{'catalog'});
	    }
	    if ($storage->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($storage);
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($chg);
	    }

	    $chg->load(
		'label'  => $label,
		'res_cb' => $steps->{'loaded'});
	} else {
            return $steps->{'scrub_db'}->();
	}
    };

    step loaded => sub {
	(my $err, $res) = @_;

	if ($err) {
	    $self->user_msg($err);
	    return $steps->{'scrub_db'}->();
	}

	my $dev = $res->{'device'};
	if ($dev->status != $DEVICE_STATUS_SUCCESS) {
	    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000062,
					severity  => $Amanda::Message::ERROR,
					dev_error => $dev->error_or_status()));
	    return $steps->{'releasing'}->();
	}
	if (!$dev->property_get('full_deletion')) {
	    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000051,
					severity  => $Amanda::Message::ERROR,
					pool      => $pool,
					label     => $label));
	    return $steps->{'releasing'}->();
	}

	if (!$params{'dry_run'}) {
	    if (!$dev->erase()) {
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000053,
					severity  => $Amanda::Message::ERROR,
					pool      => $pool,
					label     => $label));
		return $steps->{'releasing'}->();
	    }
	    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000049,
					severity  => $Amanda::Message::SUCCESS,
					pool      => $pool,
					label     => $label));
	    return $res->set_label(finished_cb => sub {
		$dev->finish();

		if ($params{'external_copy'}) {
		    $dev->erase();
		} elsif ($params{'keep_label'}) {
		    # label the tape with the same label it had
		    if (!$dev->start($ACCESS_WRITE, $label, "X")) {
			$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000056,
					severity  => $Amanda::Message::ERROR,
					label     => $label,
					dev_error => $dev->status_or_error));
			return $steps->{'releasing'}->();
		    }
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000050,
					severity  => $Amanda::Message::SUCCESS,
					label     => $label));
		    return $res->set_label(label => $label,
					    finished_cb => $steps->{'releasing'});
		} else {
		    return $steps->{'releasing'}->();
		}
	    });
	} else {
	    $steps->{'releasing'}->();
	}
    };

    step releasing => sub {
	my ($err) = @_;
	$gerr = $err if !$gerr;

	return $res->release(finished_cb => $steps->{'released'}) if $res;
	return $steps->{'released'}->();
    };

    step released => sub {
	my ($err) = @_;

	$res = undef;

	$err = $gerr if defined $gerr;
	if ($err) {
	    if (!$err->isa("Amanda::Message")) {
		$err = Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1,
					severity  => $Amanda::Message::ERROR,
					message   => "$err");
	    }
	    $self->user_msg($err);
	}
	return $steps->{'scrub_db'}->();
    };

    step scrub_db => sub {
	if (!$params{'external_copy'} && !$params{'dry_run'}) {
	    my $message_code;
	    if ($params{'keep_label'}) {
		my $volume = $self->{'catalog'}->reset_volume($pool, $label);
		$message_code = 1000057;
	    } else {
		$self->{'catalog'}->remove_volume($pool, $label);
		$message_code = 1000052;
	    }
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => $message_code,
				severity  => $Amanda::Message::SUCCESS,
				label     => $label,
				pool      => $pool,
				catalog_name => $self->{'catalog'}->{'catalog_name'}));
	}

	$tmp_curinfo_file = "$AMANDA_TMPDIR/curinfo-amrmtape-" . time() . "-" . $$;
	my $config_name = get_config_name();
	if (!open(AMADMIN, "$amadmin $config_name export |")) {
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000037,
				severity  => $Amanda::Message::ERROR,
				program     => $amadmin,
				strerror    => $!,
				exit_value  => $?));
	    return $steps->{'scrub_cleanup'}->();
	}
	if (!open(CURINFO, ">$tmp_curinfo_file")) {
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000038,
				severity  => $Amanda::Message::ERROR,
				filename    => $amadmin,
				strerror    => $!,
				exit_value  => $?));
	    close AMADMIN;
	    return $steps->{'scrub_cleanup'}->();
	}

	sub info_line($) {
            print CURINFO "$_[0]";
	}

	my $host;
	my $disk;
	my $dead_level = 10;
	while(my $line = <AMADMIN>) {
            my @parts = split(/\s+/, $line);
            if ($parts[0] =~ /^CURINFO|#|(?:command|last_level|consecutive_runs|(?:full|incr)-(?:rate|comp)):$/) {
		info_line $line;
            } elsif ($parts[0] eq 'host:') {
		$host = $parts[1];
		info_line $line;
            } elsif ($parts[0] eq 'disk:') {
		$disk = $parts[1];
		info_line $line;
            } elsif ($parts[0] eq 'history:') {
		info_line $line;
            } elsif ($line eq "//\n") {
		info_line $line;
		$dead_level = 10;
            } elsif ($parts[0] eq 'stats:') {
		if (scalar(@parts) < 6 || scalar(@parts) > 8) {
		    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000039,
				severity    => $Amanda::Message::ERROR,
				host        => $host,
				disk        => $disk,
				line        => $line));
		    close CURINFO;
		    close AMADMIN;
		    return $steps->{'scrub_cleanup'}->();
		}
		my $level = $parts[1];
		my $cur_label = $parts[7];
		if (defined $cur_label and $cur_label eq $label) {
                    $dead_level = $level;
		    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000040,
				severity    => $Amanda::Message::INFO,
				host        => $host,
				disk        => $disk,
				level       => $level));
		} elsif ( $level > $dead_level ) {
		    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000040,
				severity    => $Amanda::Message::INFO,
				host        => $host,
				disk        => $disk,
				level       => $level));
		} else {
                    info_line $line;
		}
            } else {
		$self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000041,
				severity    => $Amanda::Message::ERROR,
				line        => $line));
		close CURINFO;
		close AMADMIN;
		return $steps->{'scrub_cleanup'}->();
	    }
	}

	close CURINFO;

	unless (close AMADMIN) {
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000043,
				severity    => $Amanda::Message::ERROR,
				program     => $amadmin,
				strerror    => $!,
				exit_value  => $?));
	    return $steps->{'rollback_from_curinfo'}->();
	}

	unless ($params{'dry_run'}) {
	    my $config_name = get_config_name();
            if (system("$amadmin $config_name import < $tmp_curinfo_file")) {
		$self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000044,
				severity    => $Amanda::Message::ERROR,
				program     => $amadmin,
				strerror    => $!,
				exit_value  => $?));
		return $steps->{'rollback_from_curinfo'}->();
            }
	}

	return $steps->{'scrub_clean'}->();
    };

    step rollback_from_curinfo => sub {
	unlink $tmp_curinfo_file;
	return if $params{'keep_label'};
	return $steps->{'scrub_clean'}->();
    };

    step scrub_clean => sub {
	if ($tmp_curinfo_file) {
	    unlink $tmp_curinfo_file;
	    $tmp_curinfo_file = undef;
	}

	return $steps->{'start'}->(); # or abort;
    };

    step cleanup => sub {
	if ($params{'cleanup'} && !$params{'dry_run'}) {
	    my $config_name = get_config_name();
            if (system($amtrmlog, $config_name)) {
		return $steps->{'done'}->(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 1000054,
			severity    => $Amanda::Message::ERROR,
			errno       => $!,
			child_error => $?));
            }
            if (system($amtrmidx, $config_name)) {
		return $steps->{'done'}->(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 1000055,
			severity    => $Amanda::Message::ERROR,
			errno       => $!,
			child_error => $?));
            }
	}
	return $steps->{'done'}->();
    };

    step done => sub {
	my $err = shift;

	$self->user_msg($err) if $err;

	return $finished_cb->();
    };
}

sub reuse {
    my $self = shift;
    my %params = @_;

    my $finished_cb = $params{'finished_cb'};

    my $label;
    my @labels = @{$params{'labels'}};

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	$label = shift @{$params{'labels'}};

	if (!defined $label) {
	    return $steps->{'write_tapelist'}->();
        }

	$self->user_msg($self->{'catalog'}->volume_set_reuse(
				$self->{'storage'}->{'tapepool'}, $label, 1));
        return $steps->{'start'}->();
    };

    step write_tapelist => sub {
	return $self->{'storage'}->{'chg'}->set_reuse(
				labels => \@labels,
				finished_cb => $steps->{'reuse_done'});
    };

    step reuse_done => sub {
	my $err = shift;
	if ($err and $err->notimpl) {
	    $err = undef;
	}

	$self->user_msg($err) if $err;
	$finished_cb->();
    }
}


sub no_reuse {
    my $self = shift;
    my %params = @_;

    my $finished_cb = $params{'finished_cb'};

    my $label;
    my @labels = @{$params{'labels'}};

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	$label = shift @{$params{'labels'}};

	if (!defined $label) {
	    return $steps->{'write_tapelist'}->();
        }

	$self->user_msg($self->{'catalog'}->volume_set_reuse(
				$self->{'storage'}->{'tapepool'}, $label, 0));
        return $steps->{'start'}->();
    };

    step write_tapelist => sub {
	return $self->{'storage'}->{'chg'}->set_no_reuse(
				labels => \@labels,
				finished_cb => $steps->{'reuse_done'});
    };

    step reuse_done => sub {
	my $err = shift;
	if ($err and $err->notimpl) {
	    $err = undef;
	}

	$self->user_msg($err) if $err;
	$finished_cb->();
    }
}

1;
