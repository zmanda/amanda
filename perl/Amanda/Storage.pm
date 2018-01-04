# Copyright (c) 2012-2012 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94085, or: http://www.zmanda.com

package Amanda::Storage;

use strict;
use warnings;
use Data::Dumper;
use vars qw( @ISA );
use IPC::Open2;

use Amanda::Paths;
use Amanda::Util;
use Amanda::Config qw( :getconf );
use Amanda::Device qw( :constants );
use Amanda::Debug qw( debug );
use Amanda::MainLoop;
use Amanda::Policy;
require Amanda::Changer;
use Amanda::Paths;

=head1 NAME

Amanda::Storage -- interface to storage scripts

=head1 SYNOPSIS

    use Amanda::Storage;

    # load defaulf storage and changer
    my $storage = Amanda::Storage->new(catalog => $catalog);
    # load specific storage and changer
    my $storage = Amanda::Storage->new(storage_name => $storage,
				       changer_name => $changer,
				       catalog      => $catalog);
    my $chg = $storage->{'chg'};
    $storage->quit();

=head1 INTERFACE

All operations in the module return immediately, and take as an argument a
callback function which will indicate completion of the changer operation -- a
kind of continuation.  The caller should run a main loop (see
L<Amanda::MainLoop>) to allow the interactions with the changer script to
continue.

A new object is created with the C<new> function as follows:

  my $storage = Amanda::Storage->new(storage_name   => $storage_name,
				     changer_name   => $changer_name,
				     catalog        => $catalog,
				     labelstr       => $labelstr,
				     autolabel      => $autolabel,
				     meta_autolabel => $meta_autolabel);

to create a named storage (a name provided by the user, either specifying a
storage directly or specifying a storage definition).

If there is a problem creating the new object, then the result is a
Amanda::Changer::Error.

Thus the usual recipe for creating a new storage is

  my $storage = Amanda::Storage->new(catalog => $catalog);
  if ($storage->isa("Amanda::Changer::Error")) {
    die("Error creating storage: $sorage");
  }
  my $chg = $storage->{'chg'};
  if ($chg->isa("Amanda::Changer::Error")) {
    die("Error creating changer:  $chg");
  }

C<catalog> must be an Amanda::DB::Catalog2 object. It is required if you want to
use $chg->volume_is_labelable(), $chg->make_new_tape_label(),
$chg->make_new_meta_label(), $res->make_new_tape_label() or
$res->make_new_meta_label().
C<labelstr> must be like getconf($CNF_LABELSTR), that value is used if C<labelstr> is not set.
C<autolabel> must be like getconf($CNF_AUTOLABEL), that value is used if C<autolabel> is not set.
C<meta_autolabel> must be like getconf($CNF_META_AUTOLABEL), that value is used if C<meta_autolabel> is not set.
=head2 MEMBER VARIABLES

Note that these variables are not set until after the subclass constructor is
finished.

=over 4

=item C<< $storage>{'storage_name'} >>

Gives the name of the storage.  This name will make sense to the user.
It should be used to describe the storage in messages to the user.

=cut

sub DESTROY {
    my $self = shift;

    debug("Storage '$self->{'storage_name'}' not quit") if defined $self->{'storage_name'};
}

# this is a "virtual" constructor which instantiates objects of different
# classes based on its argument.  Subclasses should not try to chain up!
sub new {
    my $class = shift;
    $class eq 'Amanda::Storage'
	or die("Do not call the Amanda::Storage constructor from subclasses");
    my %params = @_;
#die("No catalog in Storage->new") if !defined $params{'catalog'};
    my $storage_name = $params{'storage_name'};
    my $changer_name = $params{'changer_name'};
    my ($uri, $cc);

    if (defined $changer_name and !$changer_name) {
        return Amanda::Changer::Error->new('fatal',
		source_filename => __FILE__,
		source_line     => __LINE__,
		code            => 1150000,
		storage_name	=> $storage_name,
		severity	=> $Amanda::Message::ERROR);
    }

    if (!defined $storage_name) {
	my $il = getconf($CNF_STORAGE);
	$storage_name = $il->[0];
    }

    my $self = undef;

    # Create a storage
    if (!$storage_name) {
	return Amanda::Changer::Error->new('fatal',
		source_filename => __FILE__,
		source_line     => __LINE__,
		code            => 1150001,
		storage_name	=> $storage_name,
		severity	=> $Amanda::Message::ERROR);
    }
    my $st = Amanda::Config::lookup_storage($storage_name);
    if (!$st) {
	return Amanda::Changer::Error->new('fatal',
		source_filename => __FILE__,
		source_line     => __LINE__,
		code            => 1150002,
		severity	=> $Amanda::Message::ERROR,
		storage_name	=> $storage_name);
    }

    my $tpchanger = storage_getconf($st, $STORAGE_TPCHANGER);
    if (!exists $params{'changer_name'}) {
	$changer_name = $tpchanger if !$changer_name;
	if (!$changer_name || $changer_name eq '') {
	    my $tapedev = storage_getconf($st, $STORAGE_TAPEDEV);
	    if (!$tapedev || $tapedev eq '') {
		return Amanda::Changer::Error->new('fatal',
		    source_filename => __FILE__,
		    source_line     => __LINE__,
		    code            => 1150003,
		    severity	    => $Amanda::Message::ERROR,
		    storage_name    => $storage_name);
	    }
	    $changer_name = $tapedev;
	}
    }

    $self = {
	storage_name   => $storage_name,
    };
    $self->{'labelstr'} = storage_getconf($st, $STORAGE_LABELSTR);
    $self->{'autolabel'} = storage_getconf($st, $STORAGE_AUTOLABEL);
    $self->{'meta_autolabel'} = storage_getconf($st, $STORAGE_META_AUTOLABEL);
    $self->{'tpchanger'} = $tpchanger;
    $self->{'runtapes'} = storage_getconf($st, $STORAGE_RUNTAPES);
    $self->{'taperscan_name'} = storage_getconf($st, $STORAGE_TAPERSCAN);
    $self->{'tapetype_name'} = storage_getconf($st, $STORAGE_TAPETYPE);
    $self->{'max_dle_by_volume'} = storage_getconf($st, $STORAGE_MAX_DLE_BY_VOLUME);
    $self->{'taperalgo'} = storage_getconf($st, $STORAGE_TAPERALGO);
    $self->{'taper_parallel_write'} = storage_getconf($st, $STORAGE_TAPER_PARALLEL_WRITE);
    $self->{'policy'} = Amanda::Policy->new(policy => storage_getconf($st, $STORAGE_POLICY));
    $self->{'tapepool'} = storage_getconf($st, $STORAGE_TAPEPOOL);
    $self->{'eject_volume'} = storage_getconf($st, $STORAGE_EJECT_VOLUME);
    $self->{'erase_volume'} = storage_getconf($st, $STORAGE_ERASE_VOLUME);
    $self->{'device_output_buffer_size'} = storage_getconf($st, $STORAGE_DEVICE_OUTPUT_BUFFER_SIZE);
    $self->{'seen_device_output_buffer_size'} = storage_seen($st, $STORAGE_DEVICE_OUTPUT_BUFFER_SIZE);
    $self->{'autoflush'} = storage_getconf($st, $STORAGE_AUTOFLUSH);
    $self->{'flush_threshold_dumped'} = storage_getconf($st, $STORAGE_FLUSH_THRESHOLD_DUMPED);
    $self->{'flush_threshold_scheduled'} = storage_getconf($st, $STORAGE_FLUSH_THRESHOLD_SCHEDULED);
    $self->{'taperflush'} = storage_getconf($st, $STORAGE_TAPERFLUSH);
    $self->{'report_use_media'} = storage_getconf($st, $STORAGE_REPORT_USE_MEDIA);
    $self->{'report_next_media'} = storage_getconf($st, $STORAGE_REPORT_NEXT_MEDIA);
    $self->{'interactivity'} = storage_getconf($st, $STORAGE_INTERACTIVITY);
    $self->{'set_no_reuse'} = storage_getconf($st, $STORAGE_SET_NO_REUSE);
    $self->{'dump_selection'} = storage_getconf($st, $STORAGE_DUMP_SELECTION);
    $self->{'erase_on_failure'} = storage_getconf($st, $STORAGE_ERASE_ON_FAILURE);
    $self->{'erase_on_full'} = storage_getconf($st, $STORAGE_ERASE_ON_FULL);
    bless $self, $class;

    $self->{'tapetype'} = lookup_tapetype($self->{'tapetype_name'});
    $self->{'chg'} = Amanda::Changer->new($changer_name, storage => $self,
					  catalog => $params{'catalog'},
					  no_validate => $params{'no_validate'})
				if defined $changer_name && !$params{'no_changer'};
    return $self;

}

sub erase_no_retention {
    my $self = shift;

    my @command = ("$sbindir/amrmtape", Amanda::Config::get_config_name(), "--remove-no-retention", "--keep-label", "--erase", "-ostorage=$self->{'storage_name'}");
    debug("Running: " . join(' ', @command));
    my ($child_out, $child_in);
    my $pid = open2($child_out, $child_in, @command);
    close($child_in);
    while (<$child_out>) {
	debug($_);
    }
    close($child_out);
    waitpid($pid, 0);
}

sub quit {
    my $self = shift;

    delete $self->{'storage_name'};
    delete $self->{'labelstr'};
    delete $self->{'autolabel'};
    delete $self->{'tpchanger'};
    $self->{'chg'}->quit() if defined $self->{'chg'};
    delete $self->{'chg'};
}

1;
