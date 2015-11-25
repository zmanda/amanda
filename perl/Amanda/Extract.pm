# vim:ft=perl
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Amanda::Extract;

use strict;
use warnings;
use IPC::Open3;
use IPC::Open2;

use File::Basename;
use Amanda::Debug qw( :logging );
use Amanda::Paths;
use Amanda::Config qw( :getconf );
use Amanda::Constants;
use Amanda::Util;

=head1 NAME

Amanda::Extract - perl utilities to run scripts and applications

=head1 SYNOPSIS

  use Amanda::Extract;

  my $extract = Amanda::Extract->new(hdr => $hdr,
				     dle => $dle);

  my (@bsu, @err)= $extract->BSU();
  # @err can be an array of string
  # @bsu can be an Amanda::Message on error

  # to do an extraction:
  $extract->set_restore_argv(directory => $directory,
			     use_dar   => $use_dar,
			     state_filename => $state_filename,
			     application_property => \@application_property);
  # you can fork/exec $extract->{'restore_argv'}


  # to do a validation:
  $extract->set_validate_argv(directory => $directory,
			      use_dar   => $use_dar,
			      state_filename => $state_filename,
			      application_property => \@application_property);
  # you can fork/exec $extract->{'restore_argv'}
  #

=cut

package Amanda::Extract::Message;
use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 4800000) {
	return "Unknown program '$self->{'program'}' in header";
    } elsif ($self->{'code'} == 4800001) {
	return "Application not set in header";
    } elsif ($self->{'code'} == 4800002) {
	return "Application '$self->{application} ($self->{'program_path'})' not available on the server";
    } elsif ($self->{'code'} == 4800003) {
	return "ERROR: XML error: $self->{'xml_error'}\n$self->{'dle_str'}";
    } else {
	return "No mesage for code '$self->{'code'}'";
    }
}

package Amanda::Extract;

my %restore_programs = (
    "STAR" => [ $Amanda::Constants::STAR, qw(-x -f -) ],
    "DUMP" => [ $Amanda::Constants::RESTORE, qw(xbf 2 -) ],
    "VDUMP" => [ $Amanda::Constants::VRESTORE, qw(xf -) ],
    "VXDUMP" => [ $Amanda::Constants::VXRESTORE, qw(xbf 2 -) ],
    "XFSDUMP" => [ $Amanda::Constants::XFSRESTORE, qw(-v silent) ],
    "TAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -xf -) ],
    "GTAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -xf -) ],
    "GNUTAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -xf -) ],
    "SMBCLIENT" => [ $Amanda::Constants::GNUTAR, qw(-xf -) ],
    "PKZIP" => undef,
);

my %validate_programs = (
    "STAR" => [ $Amanda::Constants::STAR, qw(-t -f -) ],
    "DUMP" => [ $Amanda::Constants::RESTORE, qw(tbf 2 -) ],
    "VDUMP" => [ $Amanda::Constants::VRESTORE, qw(tf -) ],
    "VXDUMP" => [ $Amanda::Constants::VXRESTORE, qw(tbf 2 -) ],
    "XFSDUMP" => [ $Amanda::Constants::XFSRESTORE, qw(-t -v silent) ],
    "TAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -tf -) ],
    "GTAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -tf -) ],
    "GNUTAR" => [ $Amanda::Constants::GNUTAR, qw(--ignore-zeros -tf -) ],
    "SMBCLIENT" => [ $Amanda::Constants::GNUTAR, qw(-tf -) ],
    "PKZIP" => undef,
);

sub new {
    my $class = shift;
    my %params = @_;

    my $self = bless {
	hdr => $params{'hdr'},
	dle => $params{'dle'}
    }, $class;

   return $self;
}

sub _set_program_path {
    my $self = shift;

    return $self->{'program_path'} if $self->{'program_path'};

    my $program = uc(basename($self->{'hdr'}->{program}));
    if ($program ne "APPLICATION") {
	$self->{'program_is_application'} = 0;
	if (!exists $restore_programs{$program}) {
	    return Amanda::Extract::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4800000,
				severity        => $Amanda::Message::ERROR,
				program         => $program);
	}
	return $restore_programs{$program}[0];
    }

    $self->{'program_is_application'} = 1;
    if (!defined $self->{'hdr'}->{application}) {
	return Amanda::Extract::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4800001,
				severity        => $Amanda::Message::ERROR);
    }
    my $program_path = $Amanda::Paths::APPLICATION_DIR . "/" .
                                   $self->{'hdr'}->{application};
    if (!-x $program_path) {
	return Amanda::Extract::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code            => 4800002,
				severity        => $Amanda::Message::ERROR,
				application     => $self->{'hdr'}->{'application'},
				program_path    => $program_path);
    }

    $self->{'program_path'} = $program_path;
}

sub BSU {
    my $self = shift;
    my $config = Amanda::Config::get_config_name();
    my $program_path = $self->_set_program_path();

    return undef if !$self->{'program_is_application'};
    return $program_path if $program_path->isa('Amanda::Message');

    my %bsu;
    my @err;
    my @command;

    push @command, $program_path;
    push @command, "support";
    push @command, "--config", $config if $config;
    push @command, "--host"  , $self->{'hdr'}->{'name'}   if $self->{'hdr'}->{'name'};
    push @command, "--disk"  , $self->{'hdr'}->{'disk'}   if $self->{'hdr'}->{'disk'};
    push @command, "--device", $self->{'dle'}->{'diskdevice'} if $self->{'dle'}->{'diskdevice'};
    debug("Running: " . join(' ', @command));

    my $in;
    my $out;
    my $err = Symbol::gensym;
    my $pid = open3($in, $out, $err, @command);

    close($in);
    while (my $line = <$out>) {
	chomp $line;
	debug("support: $line");
	my ($name, $value) = split ' ', $line;

	$name = lc($name);

	if ($name eq 'config' ||
	    $name eq 'host' ||
	    $name eq 'disk' ||
	    $name eq 'index-line' ||
	    $name eq 'index-xml' ||
	    $name eq 'message-line' ||
	    $name eq 'message-json' ||
	    $name eq 'message-xml' ||
	    $name eq 'record' ||
	    $name eq 'include-file' ||
	    $name eq 'include-list' ||
	    $name eq 'include-list-glob' ||
	    $name eq 'include-optional' ||
	    $name eq 'exclude-file' ||
	    $name eq 'exclude-list' ||
	    $name eq 'exclude-list-glob' ||
	    $name eq 'exclude-optional' ||
	    $name eq 'collection' ||
	    $name eq 'caclsize' ||
	    $name eq 'client-estimate' ||
	    $name eq 'multi-estimate' ||
	    $name eq 'amfeatures' ||
	    $name eq 'recover-dump-state-file' ||
	    $name eq 'dar') {
	    $bsu{$name} = ($value eq "YES");
	} elsif ($name eq 'max-level') {
	    $bsu{$name} = $value;
	} elsif ($name eq 'recover-mode') {
	    $bsu{'smb-recover-mode'} = $value eq 'SMB';
	} elsif ($name eq 'recover-path') {
	    $bsu{'recover-path-cwd'} = $value eq 'CWD';
	    $bsu{'recover-path-remote'} = $value eq 'REMOTE';
	} elsif ($name eq 'data-path') {
	    if ($value eq 'AMANDA') {
		$bsu{'data-path-amanda'} = 1;
	    } elsif ($value eq 'DIRECTTCP') {
		$bsu{'data-path-directtcp'} = 1;
	    }
	}
    }
    close($out);

    while (my $line = <$err>) {
	chomp($line);
	next if $line == '';
	push @err, $line;
    }
    close($err);

    waitpid($pid, 0);
    my $child_exit_status = $? >> 8;

    if ($child_exit_status != 0) {
	push @err, "exited with status $child_exit_status";
    }
    $self->{'bsu'} = \%bsu;
    return (\%bsu, \@err);
}

sub set_argv {
    my $self = shift;
    my $action = shift;
    my %params = @_;

    my $config = Amanda::Config::get_config_name();
    my $program_path = $self->_set_program_path();

    return $program_path if ref $program_path eq "HASH" && $program_path->isa('Amanda::Message');

    if (!$self->{'program_is_application'}) {
	my $program = uc(basename($self->{'hdr'}->{program}));
	if ($action eq "restore") {
	    return $restore_programs{$program};
	} else {
	    return $validate_programs{$program};
	}
	return;
    }

    my @argv;
    if ($action eq "restore") {
	push @argv, $program_path, "restore";
    } else {
	push @argv, $program_path, "validate";
    }
    push @argv, "--config", $config if defined $config;
    push @argv, "--host", $self->{'hdr'}->{'name'};
    push @argv, "--disk", $self->{'hdr'}->{'disk'};
    push @argv, "--device", $self->{'dle'}->{'diskdevice'} if defined ($self->{'dle'}->{'diskdevice'});
    push @argv, "--level", $self->{'hdr'}->{'dumplevel'};
    push @argv, "--directory", $params{'directory'} if $params{'directory'};
    push @argv, "--dar", "YES" if $params{'use_dar'};

   if ($self->{'bsu'}->{'recover-dump-state-file'} &&
	$params{'state_filename'}) {
	push @argv, "--recover-dump-state-file", $params{'state_filename'};
   } else {
	my $host = Amanda::Util::sanitise_filename("".$self->{'hdr'}->{'name'});
	my $disk = Amanda::Util::sanitise_filename("".$self->{'hdr'}->{'disk'});
	my $state_filename = getconf($CNF_INDEXDIR) . '/' . $host .
			'/' . $disk . '/' . $self->{'hdr'}->{'datestamp'} .
			'_' .  $self->{'hdr'}->{'dumplevel'} . '.state';
	if (-e $state_filename) {
	    push @argv, "--recover-dump-state-file",
	    $state_filename;
	} else {
	    my $state_filename_gz = $state_filename . $Amanda::Constants::COMPRESS_SUFFIX;
	    if (-e $state_filename_gz) {
		open STATEFILE, '>', $state_filename;
		my $pid = open2(">&STATEFILE", undef,
				$Amanda::Constants::UNCOMPRESS_PATH,
				$Amanda::Constants::UNCOMPRESS_OPT,
				$state_filename_gz);
		close STATEFILE;
		waitpid($pid, 0);
		push @argv, "--recover-dump-state-file",
		$state_filename;
	    }
	}
    }

    # add application_property
    while (my($name, $values) = each(%{$params{'application_property'}})) {
	if (UNIVERSAL::isa( $values, "ARRAY" )) {
	    foreach my $value (@{$values}) {
		push @argv, "--".$name, $value if defined $value;
	    }
	} else {
	    push @argv, "--".$name, $values;
	}
    }

    # merge property from header
    if (exists $self->{'dle'}->{'backup-program'}->{'property'}->{'name'} and
	!UNIVERSAL::isa($self->{'dle'}->{'backup-program'}->{'property'}->{'name'}, "HASH")) {
	# header have one property
	my $name = $self->{'dle'}->{'backup-program'}->{'property'}->{'name'};
	if (!exists $params{'application_property'}{$name}) {
	    my $values = $self->{'dle'}->{'backup-program'}->{'property'}->{'value'};
	    if (UNIVERSAL::isa( $values, "ARRAY" )) {
		# multiple values
		foreach my $value (@{$values}) {
		    push @argv, "--".$name, $value if defined $value;
		}
	   } else {
		# one value
		push @argv, "--".$name, $values;
	    }
	}
    } elsif (exists $self->{'dle'}->{'backup-program'}->{'property'}) {
	# header have multiple properties
	while (my($name, $values) =
			each (%{$self->{'dle'}->{'backup-program'}->{'property'}})) {
	    if (!exists $params{'application_property'}{$name}) {
		if (UNIVERSAL::isa( $values->{'value'}, "ARRAY" )) {
		    # multiple values
		    foreach my $value (@{$values->{'value'}}) {
			push @argv, "--".$name, $value if defined $value;
		    }
		} else {
		    # one value
		    push @argv, "--".$name, $values->{'value'};
		}
	    }
	}
    }

    return \@argv;
}

sub set_restore_argv {
    my $self = shift;
    my %params = @_;

    $self->{'restore_argv'} = $self->set_argv('restore', %params);
}

sub set_validate_argv {
    my $self = shift;
    my %params = @_;

    $self->{'validate_argv'} = $self->set_argv('validate', %params);
}

1;
