# Copyright (c) 2013 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Rest::Dles;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Device qw( :constants );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Header;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Label;
use Amanda::Curinfo;
use Amanda::Rest::Configs;
use Symbol;
use Data::Dumper;
use URI::Escape;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Dles -- Rest interface to Amanda::Curinfo and other
		     DLE functionnalities.

=head1 INTERFACE

=over

=item Changed setting on Dles

request:
  POST /amanda/v1.0/configs/:CONF/dles/hosts/:HOST
    query arguments:
        disk=DISK
        force=0|1
        force_level_1=0|1
        force_bump=0|1
        force_no_bump=0|1

reply:
  HTTP status: 200 OK
  [
     {
        "code" : "1300003",
        "disk" : "/bootAMGTAR",
        "host" : "localhost.localdomain",
        "message" : "localhost.localdomain:/bootAMGTAR is set to a forced level 0 at next run.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Curinfo.pm",
        "source_line" : "336"
     }
  ]
  [
     {
        "code" : "1300019",
        "disk" : "/bootAMGTAR",
        "host" : "localhost.localdomain",
        "message" : "force command for localhost.localdomain:/bootAMGTAR cleared.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Curinfo.pm",
        "source_line" : "365"
     }
  ]
  [
     {
        "code" : "1300021",
        "disk" : "/bootAMGTAR",
        "host" : "localhost.localdomain",
        "message" : "no force command outstanding for localhost.localdomain:/bootAMGTAR, unchanged.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Curinfo.pm",
        "source_line" : "390"
     }
  ]
  [
     {
        "code" : "1300022",
        "disk" : "/bootAMGTAR",
        "host" : "localhost.localdomain",
        "message" : "localhost.localdomain:/bootAMGTAR FORCE command was cleared",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Curinfo.pm",
        "source_line" : "466"
     }
  ]
  [
     {
        "code" : "1300023",
        "disk" : "/bootAMGTAR",
        "host" : "localhost.localdomain",
        "message" : "localhost.localdomain:/bootAMGTAR is set to a forced level 1 at next run.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Curinfo.pm",
        "source_line" : "434"
     }
  ]
  [
     {
        "code" : "1300020",
        "disk" : "/bootAMGTAR",
        "host" : "localhost.localdomain",
        "message" : "force-level-1 command for localhost.localdomain:/bootAMGTAR cleared.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Curinfo.pm",
        "source_line" : "380"
     }
  ]
  [
     {
        "code" : "1300025",
        "disk" : "/bootAMGTAR",
        "host" : "localhost.localdomain",
        "message" : "localhost.localdomain:/bootAMGTAR is set to bump at next run.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Curinfo.pm",
        "source_line" : "486"
     }
  ]
  [
     {
        "code" : "1300027",
        "disk" : "/bootAMGTAR",
        "host" : "localhost.localdomain",
        "message" : "bump command for localhost.localdomain:/bootAMGTAR cleared.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Curinfo.pm",
        "source_line" : "547"
     }
  ]
  [
     {
        "code" : "1300026",
        "disk" : "/bootAMGTAR",
        "host" : "localhost.localdomain",
        "message" : "localhost.localdomain:/bootAMGTAR is set to not bump at next run.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Curinfo.pm",
        "source_line" : "520"
     }
  ]


=back

=cut

sub setting {
    my %params = @_;
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    if (defined $params{'DISK'} and !defined $params{'disk'}) {
	$params{'disk'} = uri_unescape($params{'DISK'});
    }

print STDERR "HOST: $params{'HOST'}\n";
print STDERR "disk: $params{'disk'}\n";
    my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
    Amanda::Disklist::unload_disklist();
    my $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400006,
			diskfile     => $diskfile,
			cfgerr_level => $cfgerr_level);
	return \@result_messages;
    }

    my $curinfodir = getconf($CNF_INFOFILE);;
    my $ci = Amanda::Curinfo->new($curinfodir);
    my $host = Amanda::Disklist::get_host($params{'HOST'});
    if (!$host) { # $host->isa("Amanda::Message");
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400007,
			diskfile     => $diskfile,
			host         => $params{'HOST'});
	return \@result_messages;
    }
    my @disks;
    if ($params{'disk'}) {
	my $disk = $host->get_disk($params{'disk'});
	if (!$disk) {  # $disk->isa("Amanda::Message");
	    push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400008,
			diskfile     => $diskfile,
			host         => $params{'HOST'},
			disk         => $params{'disk'});
	    return \@result_messages;
	}
	push @disks, $disk;
    } else {
	@disks = $host->all_disks();
    }

    for my $disk (@disks) {
	if (defined $params{'force'}) {
	    if ($params{'force'}) {
		push @result_messages, $ci->force($disk);
	    } else {
		push @result_messages, $ci->unforce($disk);
	    }
	}
	if (defined $params{'force_level_1'}) {
	    if ($params{'force_level_1'}) {
		push @result_messages, $ci->force_level_1($disk);
	    } else {
		push @result_messages, $ci->unforce($disk);
	    }
	}
	if (defined $params{'force_bump'}) {
	    if ($params{'force_bump'}) {
		push @result_messages, $ci->force_bump($disk);
	    } else {
		push @result_messages, $ci->unforce_bump($disk);
	    }
	}
	if (defined $params{'force_no_bump'}) {
	    if ($params{'force_no_bump'}) {
		push @result_messages, $ci->force_no_bump($disk);
	    } else {
		push @result_messages, $ci->unforce_bump($disk);
	    }
	}
    }
    return \@result_messages;
}

1;
