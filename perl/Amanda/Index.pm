# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Index::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 2400000) {
        return "Can't find header filename for the dump";
    } elsif ($self->{'code'} == 2400001) {
        return "header file '$self->{'filename'} does not exist";
    } elsif ($self->{'code'} == 2400002) {
        return "header file '$self->{'filename'} is not a regular file";
    } elsif ($self->{'code'} == 2400003) {
        return "Can'open header file '$self->{'filename'}: $self->{'error'}";
    } elsif ($self->{'code'} == 2400004) {
        return "The header buffer";
    } elsif ($self->{'code'} == 2400005) {
        return "The header";
    } elsif ($self->{'code'} == 2400006) {
        return "The index";
    }
}

package Amanda::Index;

use strict;
use warnings;
use POSIX ();
use Fcntl qw( O_RDWR O_CREAT LOCK_EX LOCK_NB );
use Data::Dumper;
use vars qw( @ISA );
use Time::Local;
use Text::ParseWords;
use IPC::Open2;

use Amanda::Paths;
use Amanda::Util qw( match_labelstr );
use Amanda::Config qw( :getconf );
use Amanda::Device qw( :constants );
use Amanda::Debug qw( debug );
use Amanda::MainLoop;
use Amanda::Process;

=head1 NAME

Amanda::Index -- Get index or header from index directory

=head1 SYNOPSIS

    use Amanda::Index;

    my $index = Amanda::Index->new();
    my $index = Amanda::Index->new(indexdir => $indexdir);

    my $h_name = $index->get_header_filename(host      => $host,
					     disk      => $disk,
					     datestamp => $datestamp,
					     level     => $level);
    my $buffer = $index->get_header_buffer(host      => $host,
					   disk      => $disk,
					   datestamp => $datestamp,
					   level     => $level);
    my $header = $index->get_header(host      => $host,
				    disk      => $disk,
				    datestamp => $datestamp,
				    level     => $level);
    my $index_handle = $index->get_index_handle(host      => $host,
					        disk      => $disk,
					        datestamp => $datestamp,
					        level     => $level);
    my $index_data = $index->get_index(host      => $host,
				       disk      => $disk,
				       datestamp => $datestamp,
				       level     => $level);
=cut

sub new {
    my $class = shift;
    my %params = @_;

    my $indexdir = $params{'indexdir'};
    $indexdir = getconf($CNF_INDEXDIR) if !$indexdir;

    my $self = {
	indexdir => $indexdir,
    };

    bless $self, $class;
    return $self;
}

sub get_header_filename() {
    my $self = shift;
    my %params = @_;

    return Amanda::Logfile::getheaderfname($params{'host'},
					   $params{'disk'},
					   $params{'datestamp'},
					   0+$params{'level'});
}

sub get_header_buffer() {
    my $self = shift;

    my $filename = $self->get_header_filename(@_);
    if (!$filename) {
	return Amanda::Index::Message->new(
			source_filename => __FILE__,
                        source_line => __LINE__,
                        code        => 2400000,
                        filename    => $filename);
    }
    if (!-e $filename) {
	return Amanda::Index::Message->new(
			source_filename => __FILE__,
                        source_line => __LINE__,
                        code        => 2400001,
                        filename    => $filename);
    }
    if (!-f _) {
	return Amanda::Index::Message->new(
			source_filename => __FILE__,
                        source_line => __LINE__,
                        code        => 2400002,
                        filename    => $filename);
    }
    open my $fh, "<", $filename ||
	return Amanda::Index::message->new(
			source_filename => __FILE__,
                        source_line => __LINE__,
                        code        => 2400003,
			error       => $!,
                        filename    => $filename);
    my $buffer;
    sysread $fh, $buffer, 32768;
    close($fh);

    return $buffer;
}

sub get_header() {
    my $self = shift;

    my $buffer = $self->get_header_buffer(@_);
    return $buffer if $buffer->isa("Amanda::Message");

    my $hdr = Amanda::Header->from_string($buffer);

    return $hdr;
}

sub get_index_handle() {
    my $self = shift;
    my %params = @_;

    my $filename;
    my $need_uncompress;
    my $need_sort;
    $filename = Amanda::Logfile::getindex_sorted_fname($params{'host'},
						       $params{'disk'},
						       $params{'datestamp'},
						       0+$params{'level'});
    if (-f $filename) {
    } else {
	$filename = Amanda::Logfile::getindex_sorted_gz_fname(
				$params{'host'},
				$params{'disk'},
				$params{'datestamp'},
				0+$params{'level'});
	if (-f $filename) {
	    $need_uncompress = 1;
	} else {
	    $filename = Amanda::Logfile::getindex_unsorted_fname(
				$params{'host'},
				$params{'disk'},
				$params{'datestamp'},
				0+$params{'level'});
	    if (-f $filename) {
		$need_sort = 1;
	    } else {
		$filename = Amanda::Logfile::getindex_unsorted_gz_fname(
				$params{'host'},
				$params{'disk'},
				$params{'datestamp'},
				0+$params{'level'});
		if (-f $filename) {
		    $need_uncompress = 1;
		    $need_sort = 1;
		} else {
		    $filename = Amanda::Logfile::getindexfname(
				$params{'host'},
				$params{'disk'},
				$params{'datestamp'},
				0+$params{'level'});
		    if (-f $filename) {
			$need_uncompress = 1;
			$need_sort = 1;
		    }
		}
	    }
	}
    }

    if (!defined $need_uncompress && !defined $need_sort) {
	open my $h, "<", $filename;
	return $h;
    }
    my $pid_uc;
    if (defined $need_uncompress && !defined $need_sort) {
	#$pid_uc = open2($h1, undef, $Amanda::Constants::UNCOMPRESS_PATH,
	#			 $Amanda::Constants::UNCOMPRESS_OPT,
	#			 $filename);
	open my $h1, "-|", $Amanda::Constants::UNCOMPRESS_PATH,
			   $Amanda::Constants::UNCOMPRESS_OPT,
			   $filename;
	return $h1;
    }

    my $h1;
    if (defined $need_uncompress) {
	$pid_uc = open2(\*AAAA, undef, $Amanda::Constants::UNCOMPRESS_PATH,
				 $Amanda::Constants::UNCOMPRESS_OPT,
				 $filename);
    } else {
	open (AAAA, "<", $filename) or die("AAAAA");
    }
    my $pid_sort;
    my $new_filename = Amanda::Logfile::getindex_sorted_fname(
				$params{'host'},
				$params{'disk'},
				$params{'datestamp'},
				0+$params{'level'});
    $pid_sort = open2(undef, "<&AAAA", $Amanda::Constants::SORT_PATH,
				"-", "-o", $new_filename,
				"-T", getconf($CNF_TMPDIR));

    if ($pid_uc) {
	waitpid $pid_uc, 0;
    }
    if ($pid_sort) {
	waitpid $pid_sort, 0;
	open $h1, "<", $new_filename;
    }
    return $h1;
}

sub get_index() {
    my $self = shift;

    my $handle = $self->get_index_handle(@_);
    my $buffer;
    {
	local $/;
	$buffer = <$handle>;
    }
    close $handle;

    return $buffer;
}

1;
