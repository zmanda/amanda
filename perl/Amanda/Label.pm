#! @PERL@
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
	return "$self->{'label'}: Can't assign because it is is the '$self->{'config'}' config";
    } elsif ($self->{'code'} == 1000001) {
	return "$self->{'label'}: Can't assign meta-label without -f, old meta-label is '$self->{'meta'}'";
    } elsif ($self->{'code'} == 1000002) {
	return "$self->{'label'}: Can't assign barcode without -f, old barcode is '$self->{'barcode'}'";
    } elsif ($self->{'code'} == 1000003) {
	return  "$self->{'label'}: Can't assign pool without -f, old pool is '$self->{'pool'}'";
    } elsif ($self->{'code'} == 1000004) {
	return "$self->{'label'}: Can't assign storage without -f, old storage is '$self->{'storage'}'";
    } elsif ($self->{'code'} == 1000005) {
	return "$self->{'label'}: Can't assign storage because it is a new labelled tape.";
    } elsif ($self->{'code'} == 1000006) {
	return "Setting $self->{'label'}";
    } elsif ($self->{'code'} == 1000007) {
	return "All labels already correctly set.";
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
	return "Found label '$self->{'label'}' but it is from tape pool '$self->{'pool'}'.";
    } elsif ($self->{'code'} == 1000016) {
	return "Found label '$self->{'label'}' but it doesn't match the labelstr '$self->{'labelstr'}->{'template'}'.";
    } elsif ($self->{'code'} == 1000017) {
	return "Volume with label '$self->{'label'}' is active and contains data from this configuration.";
    } elsif ($self->{'code'} == 1000018) {
	return "Consider using 'amrmtape' to remove volume '$self->{'label'}' from the catalog.";
    } elsif ($self->{'code'} == 1000019) {
	return "Found label '$self->{'label'}' but it is not in the tapelist file.";
    } elsif ($self->{'code'} == 1000020) {
	return "Writing label '$self->{'label'}'...";
    } elsif ($self->{'code'} == 1000021) {
	return "Checking label...";
    } elsif ($self->{'code'} == 1000022) {
	return "Success!"
    } elsif ($self->{'code'} == 1000023) {
	return "Label '$self->{'label'}' already on a volume.";
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
	return "label '$self->{'label'}' not found in tapelist file '$self->{'tapelist_filename'}'.";
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
	return "marking tape '$self->{'label'}' as reusable.";
    } elsif ($self->{'code'} == 1000046) {
	return "tape '$self->{'label'}' already reusable.";
    } elsif ($self->{'code'} == 1000047) {
	return "marking tape '$self->{'label'}' as not reusable.";
    } elsif ($self->{'code'} == 1000048) {
	return "tape '$self->{'label'}' already not reusable.";
    } elsif ($self->{'code'} == 1000049) {
	return "Erased volume with label '$self->{'label'}'.";
    } elsif ($self->{'code'} == 1000050) {
	return "Rewrote label '$self->{'label'}' to volume.";
    } elsif ($self->{'code'} == 1000051) {
	return "Can not erase '$self->{'label'} because the device doesn't support this feature";
    } elsif ($self->{'code'} == 1000052) {
	return "Removed label '$self->{'label'} from tapelist file.";
    } elsif ($self->{'code'} == 1000053) {
	return "Failed to erase volume with label '$self->{'label'}'.";
    } elsif ($self->{'code'} == 1000054) {
	return "amtrmlog exited with non-zero while scrubbing logs: $self->{'errno'} $self->{'child_error'}.";
    } elsif ($self->{'code'} == 1000055) {
	return "amtrmidx exited with non-zero while scrubbing logs: $self->{'errno'} $self->{'child_error'}.";
    } elsif ($self->{'code'} == 1000056) {
	return "Failed to rewrite label '$self->{'label'}' to volume: $self->{'dev_error'}.";
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
use Amanda::Tapelist;
use Amanda::Paths;


=head1 NAME

Amanda::Label - Amanda module to handle label on volume.

=head1 SYNOPSIS

  # create a Label class
  my $Label = Amanda::Label->new(storage  => $storage,
				 tapelist => $tl,
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

  $Label->erase(finished_cb => $cb,
		labels      => @labels,
		erase       => $erase,
		keep_label  => $keep_label,
		cleanup     => $cleanup,
		dry_run     => $dry_run);

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
	$self->{'user_msg'}->($msg);
    }
}

sub assign {
    my $self = shift;
    my %params = @_;

    my $finished_cb = $params{'finished_cb'};
    my $chg;
    my $count = 0;
    my $exit_status = 0;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {

	$chg = $self->{'storage'}->{'chg'};

	return $steps->{'assign'}->();
    };

    step assign => sub {
	my $matched = 0;
	my $changed = 0;
	$self->{'tapelist'}->reload(1);
	for my $tle (@{$self->{'tapelist'}->{'tles'}}) {
	  if ($tle->{'label'} =~ /$params{'label'}/ or
	      $tle->{'label'} eq $params{'label'}) {
		my $changed1 = 0;
		my $error = 0;
		$matched = 1;
		if ($tle->{'config'} &&
		    $tle->{'config'} ne Amanda::Config::get_config_name()) {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000000,
					label  => $tle->{'label'},
					config => $tle->{'config'}));
		    $error = 1;
		} else {
		    if (!$tle->{'config'} && $tle->{'datestamp'} ne "0") {
			$tle->{'config'} = Amanda::Config::get_config_name();
			$changed1 = 1;
		    }
		    if (defined $params{'meta'}) {
			if (defined($tle->{'meta'}) &&
			    $params{'meta'} ne $tle->{'meta'} &&
			    !$params{'force'}) {
			    $self->user_msg(Amanda::Label::Message->new(
						source_filename => __FILE__,
						source_line => __LINE__,
						code   => 1000001,
						label  => $tle->{'label'},
						meta => $tle->{'meta'}));
			    $error = 1;
			} elsif (!defined $tle->{'meta'} or
				 $tle->{'meta'} ne $params{'meta'}) {
			    $tle->{'meta'} = $params{'meta'};
			    $changed1 = 1;
			}
		    }
		    if (defined $params{'barcode'}) {
			if (defined($tle->{'barcode'}) &&
			    $params{'barcode'} ne $tle->{'barcode'} &&
			    !$params{'force'}) {
			    $self->user_msg(Amanda::Label::Message->new(
						source_filename => __FILE__,
						source_line => __LINE__,
						code    => 1000002,
						label   => $tle->{'label'},
						barcode => $tle->{'barcode'}));
			    $error = 1;
			} elsif (!defined $tle->{'barcode'} or
				 $tle->{'barcode'} ne $params{'barcode'}) {
			    $tle->{'barcode'} = $params{'barcode'};
			    $changed1 = 1;
			}
		    }
		    if (defined $params{'pool'}) {
			if (defined($tle->{'pool'}) &&
			    $params{'pool'} ne $tle->{'pool'} &&
			    !$params{'force'}) {
			    $self->user_msg(Amanda::Label::Message->new(
						source_filename => __FILE__,
						source_line => __LINE__,
						code   => 1000003,
						label  => $tle->{'label'},
						pool   => $tle->{'pool'}));
			    $error = 1;
			} elsif (!defined $tle->{'pool'} or
				 $tle->{'pool'} ne $params{'pool'}) {
			    $tle->{'pool'} = $params{'pool'};
			    $changed1 = 1;
			}
		    }
		    if (defined $params{'storage'}) {
			if (defined($tle->{'storage'}) &&
			    $params{'storage'} ne $tle->{'storage'} &&
			    !$params{'force'}) {
			    $self->user_msg(Amanda::Label::Message->new(
						source_filename => __FILE__,
						source_line => __LINE__,
						code    => 1000004,
						label   => $tle->{'label'},
						storage => $tle->{'storage'}));
			    $error = 1;
			} elsif ($tle->{'datestamp'} eq "0") {
			    $self->user_msg(Amanda::Label::Message->new(
						source_filename => __FILE__,
						source_line => __LINE__,
						code   => 1000005,
						label  => $tle->{'label'}));
			} elsif (!defined $tle->{'storage'} or
				 $tle->{'storage'} ne $params{'storage'}) {
			    $tle->{'storage'} = $params{'storage'};
			    $changed1 = 1;
			}
		    }
		}

		if ($changed1 && !$error) {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000006,
					label  => $tle->{'label'}));
		    $changed++;
		}
		$exit_status |= $error;
	    }
	}
	if ($exit_status == 1) {
	    return $finished_cb->(Amanda::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 2));
	} elsif ($changed) {
	    $self->{'tapelist'}->write();
	} elsif ($matched) {
	    $self->{'tapelist'}->unlock();
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code   => 1000007));
	} else {
	    $self->{'tapelist'}->unlock();
	    return $finished_cb->("No label matching '$params{'label'}' in the tapelist file");
	}

	return $finished_cb->() if !$params{'meta'};
	$chg->inventory(inventory_cb => $steps->{'assign_inventory'});
    };

    step assign_inventory => sub {
	my ($err, $inv) = @_;

	if ($err) {
	    return $finished_cb->() if $err->notimpl;
	    return $finished_cb->(Amanda::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code    => 1,
					message => $err));
	}

	for my $sl (@$inv) {
	    if (defined $sl->{'label'} && ($sl->{'label'} =~ /$params{'label'}/ ||
					   $sl->{'label'} eq $params{'label'})) {
		$count++;
		$chg->set_meta_label(meta => $params{'meta'},
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
	return $finished_cb->() if $count == 0;
	return $steps->{'maybe_done'}->();
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
    my $finished_cb = $params{'finished_cb'};
    my $res;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {

	$chg = $self->{'storage'}->{'chg'};

	$labelstr = $self->{'storage'}->{'labelstr'};
	$autolabel = $self->{'storage'}->{'autolabel'};
	$tapepool = $self->{'storage'}->{'tapepool'};

	if (defined($params{'label'}) && !$params{'force'}) {
	    if ($self->{'tapelist'}->lookup_tapelabel($params{'label'})) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000023,
					label  => $params{'label'}));
	    }
	}

	$steps->{'load'}->();
    };

    step load => sub {
	$self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code   => 1000008));
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
					slot        => $params{'slot'},
					barcode     => $params{'barcode'},
					red_barcode => $res->{'barcode'}));
	    } else {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code        => 1000026,
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
					code   => 1000009));
	    } else {
		# force is required for non-Amanda tapes
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000010));
		$dev_ok = 0 unless ($params{'force'});
	    }
	} elsif ($dev->status & $DEVICE_STATUS_VOLUME_ERROR) {
	    # it's OK to force through VOLUME_ERROR
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000011,
				dev_error => $dev->error_or_status()));
	    $dev_ok = 0 unless ($params{'force'});
	} elsif ($dev->status != $DEVICE_STATUS_SUCCESS) {
	    # but anything else is fatal
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000012,
				dev_error => $dev->error_or_status()));
	    $dev_ok = 0;
	} else {
	    # this is a labeled Amanda tape
	    my $label = $dev->volume_label;
	    my $barcode = $res->{'barcode'};
	    my $meta = $res->{'meta'};
	    my $tle = $self->{'tapelist'}->lookup_tapelabel($label);

	    if ($params{'label'} &&
		$labelstr->{'template'} &&
		!match_labelstr($labelstr, $autolabel, $params{'label'}, $barcode, $meta)) {
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000013,
					label     => $label,
					labelstr  => $labelstr));
		$dev_ok = 0;
	    } elsif ($tle) {
		if ($tle->{'config'} && $tle->{'config'} ne Amanda::Config::get_config_name()) {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000014,
					label     => $label,
					config    => $tle->{'config'}));
		    $dev_ok = 0;
		} elsif ($tle->{'pool'} &&
			 $tle->{'pool'} ne $tapepool) {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000015,
					label     => $label,
					pool      => $tle->{'pool'}));
		    $dev_ok = 0;
		} elsif (!$tle->{'pool'} &&
			 !match_labelstr($labelstr, $autolabel, $label, $barcode, $meta)) {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000016,
					label     => $label,
					labelstr  => $labelstr));
		    $dev_ok = 0;
		} else {
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000017,
					label     => $label));
		    if ($params{'force'}) {
			# if -f, then the user should clean things up..
			$self->user_msg(Amanda::Label::Message->new(
						source_filename => __FILE__,
						source_line => __LINE__,
						code      => 1000018,
						label     => $label));
			# note that we don't run amrmtape automatically, as it could result in data loss when
			# multiple volumes have (perhaps accidentally) the same label
		    } else {
			$dev_ok = 0
		    }
		}
	    } else {
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000019,
					label     => $label));
		$dev_ok = 0 if !$params{'force'};
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

	if ($dev_ok) {
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000020,
				label     => $label));

	    if (!$dev->start($ACCESS_WRITE, $label, "X")) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000028,
					dev_error => $dev->error_or_status()));
	    } elsif (!$dev->finish()) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000029,
					dev_error => $dev->error_or_status()));
	    }

	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000021));
	    my $status = $dev->read_label();
	    if ($status != $DEVICE_STATUS_SUCCESS) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000030,
					dev_error => $dev->error_or_status()));
	    } elsif (!$dev->volume_label) {
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000031));
	    } elsif ($dev->volume_label ne $label) {
		my $got = $dev->volume_label;
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000032,
					label     => $label,
					got_label => $got));
	    } elsif ($dev->volume_time ne "X") {
		my $got = $dev->volume_time;
		return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000033,
					got_timestamp => $got));
	    }

	    # update the tapelist
	    $self->{'tapelist'}->reload(1);
	    $self->{'tapelist'}->remove_tapelabel($label);
	    # the label is not yet assigned a config
	    $self->{'tapelist'}->add_tapelabel("0", $label, undef, 1, $meta, $res->{'barcode'},
			       $dev->block_size/1024,
			       $tapepool, undef, undef);
	    $self->{'tapelist'}->write();

	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000022));

	    # notify the changer
	    $res->set_label(label => $label, finished_cb => $steps->{'set_meta_label'});
	} else {
	    return $steps->{'releasing'}->(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000034));
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

	my $tle = $self->{'tapelist'}->lookup_tapelabel($label);
	if (!defined $tle) {
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000035,
			label  => $label,
			tapelist_filename =>$self->{'tapelist'}->{'filename'}));
	    return $steps->{'start'}->();
	}

	my $storage_name = $tle->{'storage'};
	if ($storage_name) {
	    return $steps->{'erase'}->($storage_name);
	}

	# no storage in the tapelist, use the first storage with the same pool
	if ($tle->{'pool'}) {
	    if (getconf_seen($CNF_STORAGE)) {
		my $storage_list = getconf($CNF_STORAGE);
		my $done = 0;
		for $storage_name (@{$storage_list}) {
		    if (defined $storage and $storage->{'name'} ne $storage_name) {
			$storage->quit(); $storage = undef;
			$chg->quit(); $chg = undef;
		    }
		    if (!$storage) {
			$storage = Amanda::Storage->new(
						storage_name => $storage_name,
						tapelist => $self->{'tapelist'});
			if (!$storage->isa("Amanda::Changer::Error")) {
			    if ($storage->{'tapepool'} eq $tle->{'pool'}) {
				debug("Using storage '$storage_name' because it use the same '$tle->{'pool'}' tape pool");
				return $steps->{'erase'}->($storage_name);
				$done = 1;
				last;
			    }
			}
		    }
		}
		next if $done;
	    }
	}

	# try in the default storage
	if (getconf_seen($CNF_STORAGE)) {
	    my $storage_list = getconf($CNF_STORAGE);
	    for $storage_name (@{$storage_list}) {
		return $steps->{'erase'}->($storage_name);
	    }
	    next;
	}

	# try in the config_name storage;
	$storage_name = get_config_name();
	if (defined $storage_name) {
	    return $steps->{'erase'}->($storage_name);
	}

	return $steps->{'start'}->();
    };

    step erase => sub {
	my $storage_name = shift;

	if ($params{'erase'}) {
	    if (defined $storage and $storage->{'storage_name'} ne $storage_name) {
		$storage->quit() if defined $storage; $storage = undef;
		$chg->quit() if defined $chg; $chg = undef;
	    }
	    if (!$storage) {
		$storage = Amanda::Storage->new(storage_name => $storage_name,
						tapelist => $self->{'tapelist'});
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
	    return $steps->{'start'}->();
	}

	my $dev = $res->{'device'};
	if (!$dev->property_get('full_deletion')) {
	    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000051,
					label     => $label));
	    return $steps->{'releasing'}->();
	}

	if (!$params{'dry_run'}) {
	    if (!$dev->erase()) {
		$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000053,
					label     => $label));
		return $steps->{'releasing'}->();
	    }
	    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000049,
					label     => $label));
	    return $res->set_label(finished_cb => sub {
		$dev->finish();

		# label the tape with the same label it had
		if ($params{'keep_label'}) {
		    if (!$dev->start($ACCESS_WRITE, $label, undef)) {
			$self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000056,
					label     => $label,
					dev_error => $dev->status_or_error));
			return $steps->{'releasing'}->();
		    }
		    $self->user_msg(Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code      => 1000050,
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
					message   => "$err");
	    }
	    $self->user_msg($err);
	}
	return $steps->{'scrub_db'}->();
    };

    step scrub_db => sub {
	my $tle = $self->{'tapelist'}->lookup_tapelabel($label);
	if (!defined $tle) {
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000035,
				label     => $label,
				tapelist_filename => $self->{'tapelist'}->{'filename'}));
	    return $steps->{'start'}->();
	} elsif ($params{'keep_label'}) {
            $tle->{'datestamp'} = 0 if $tle;
            $tle->{'storage'} = undef if $tle;
            $tle->{'config'} = undef if $tle;
	} else {
            $self->{'tapelist'}->remove_tapelabel($label);
	}

	#take a copy in case we roolback
	$backup_tapelist_file = dirname($self->{'tapelist'}->{'filename'}) . "-backup-amrmtape-" . time();
	if (-x $self->{'tapelist'}->{'filename'}) {
	    if (!copy($self->{'tapelist'}->{'filename'}, $backup_tapelist_file)) {
		$self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000036,
				tapelist_filename =>$self->{'tapelist'}->{'filename'},
				backup_tapelist   =>$backup_tapelist_file,
				strerror          => $!));
		return $steps->{'scrub_cleanup'}->();
	    }
	}

	unless ($params{'dry_run'}) {
            $self->{'tapelist'}->write();
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code      => 1000052,
				label     => $label,
				tapelist_filename => $self->{'tapelist'}->{'filename'}));
	}

	$tmp_curinfo_file = "$AMANDA_TMPDIR/curinfo-amrmtape-" . time() . "-" . $$;
	my $config_name = get_config_name();
	if (!open(AMADMIN, "$amadmin $config_name export |")) {
	    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000037,
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
				host        => $host,
				disk        => $disk,
				level       => $level));
		} elsif ( $level > $dead_level ) {
		    $self->user_msg(Amanda::Label::Message->new(
				source_filename => __FILE__,
				source_line => __LINE__,
				code        => 1000040,
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
	if (!move($backup_tapelist_file, $self->{'tapelist'}->{'filename'})) {
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 1000042,
			tapelist_filename=> $self->{'tapelist'}->{'filename'},
			backup_tapelist   =>$backup_tapelist_file,
			strerror    => $!,
			exit_value  => $?));
	}
	return $steps->{'scrub_clean'}->();
    };

    step scrub_clean => sub {
	if ($tmp_curinfo_file) {
	    unlink $tmp_curinfo_file;
	    $tmp_curinfo_file = undef;
	}
	if ($backup_tapelist_file) {
	    unlink $backup_tapelist_file;
	    $backup_tapelist_file = undef;
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
			errno       => $!,
			chilk_error => $?));
            }
            if (system($amtrmidx, $config_name)) {
		return $steps->{'done'}->(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 1000055,
			errno       => $!,
			chilk_error => $?));
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

    my $need_write = 0;
    my $label;
    my @labels = @{$params{'labels'}};

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	$label = shift @{$params{'labels'}};

	if (!defined $label) {
	    return $steps->{'write_tapelist'}->();
        }

	my $tle = $self->{'tapelist'}->lookup_tapelabel($label);

	if (!defined $tle) {
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000035,
			label  => $label,
			tapelist_filename =>$self->{'tapelist'}->{'filename'}));
            return $steps->{'start'}->();
        }

        if ($tle->{'reuse'} == 0) {
            $self->{'tapelist'}->reload(1) if $need_write == 0;;
            $self->{'tapelist'}->remove_tapelabel($label);
            $self->{'tapelist'}->add_tapelabel($tle->{'datestamp'},
				$label, $tle->{'comment'},
				1, $tle->{'meta'}, $tle->{'barcode'},
				$tle->{'blocksize'}, $tle->{'pool'},
				$tle->{'storage'}, $tle->{'config'});
            $need_write = 1;
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000045,
			label  => $label));
        } else {
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000046,
			label  => $label));
        }
        return $steps->{'start'}->();
    };

    step write_tapelist => sub {
	$self->{'tapelist'}->write() if $need_write == 1;

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

    my $need_write = 0;
    my $label;
    my @labels = @{$params{'labels'}};

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	$label = shift @{$params{'labels'}};

	if (!defined $label) {
	    return $steps->{'write_tapelist'}->();
        }

	my $tle = $self->{'tapelist'}->lookup_tapelabel($label);

	if (!defined $tle) {
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000035,
			label  => $label,
			tapelist_filename =>$self->{'tapelist'}->{'filename'}));
            return $steps->{'start'}->();
        }

        if ($tle->{'reuse'} == 1) {
            $self->{'tapelist'}->reload(1) if $need_write == 0;;
            $self->{'tapelist'}->remove_tapelabel($label);
            $self->{'tapelist'}->add_tapelabel($tle->{'datestamp'},
				$label, $tle->{'comment'},
				0, $tle->{'meta'}, $tle->{'barcode'},
				$tle->{'blocksize'}, $tle->{'pool'},
				$tle->{'storage'}, $tle->{'config'});
            $need_write = 1;
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000047,
			label  => $label));
        } else {
	    $self->user_msg(Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000048,
			label  => $label));
        }
        return $steps->{'start'}->();
    };

    step write_tapelist => sub {
	$self->{'tapelist'}->write() if $need_write == 1;

	return $self->{'storage'}->{'chg'}->set_no_reuse(
				labels => \@labels,
			        finished_cb => $steps->{'no_reuse_done'});
    };

    step no_reuse_done => sub {
	my $err = shift;
	if ($err and $err->notimpl) {
	    $err = undef;
	}

	$self->user_msg($err) if $err;
	$finished_cb->();
    }
}

1;
