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

package Amanda::Rest::Dles;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Device qw( :constants );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Header;
use Amanda::MainLoop;
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

=item Get a list (and setting) of all Dles

 request:
  GET /amanda/v1.0/configs/:CONF/dles
 query arguments:
  expand_application=1
  expand_dumptype=1
  expand_interface=1
  expand_script=1

 reply:
  HTTP status: 200 OK
  [
   {
      "module" : "amanda",
      "source_line" : "242",
      "severity" : "info",
      "process" : "Amanda::Rest::Dles",
      "diskfile" : "/amanda/h1/etc/amanda/test/disklist",
      "source_filename" : "/amanda/h1/linux/lib/amanda/perl/Amanda/Rest/Dles.pm",
      "component" : "rest-server",
      "message" : "List of DLEs.",
      "running_on" : "amanda-server",
      "code" : "1400010",
      "result" : [
         {
            "disk" : "/boot",
            "dumptype" : "custom(10.5.15.56:_boot).114",
            "host" : "10.5.15.56",
            "spindle" : "-1",
            "device" : "/boot"
         },
         {
            "host" : "127.0.0.1",
            "spindle" : "-1",
            "device" : "/boot",
            "disk" : "/bootAMGTAR",
            "dumptype" : "custom(127.0.0.1:_bootAMGTAR).78"
         }
      ]
   }
  ]

=item Get a list (and setting) of all Dles for a host

 request:
  GET /amanda/v1.0/configs/:CONF/dles/hosts/:HOST
 query arguments:
  expand_application=1
  expand_dumptype=1
  expand_interface=1
  expand_script=1

 reply:

=item Get a list (and setting) of a Dle

 request:
  GET /amanda/v1.0/configs/:CONF/dles/hosts/:HOST/disk/:DISK
   each '/' in the :DISK must be encoded as '%252F'
 query arguments:
  expand_application=1
  expand_dumptype=1
  expand_interface=1
  expand_script=1

 reply:

=item Change setting on Dles

 request:
  POST /amanda/v1.0/configs/:CONF/dles/hosts/:HOST/disks/:DISK
    each '/' in the :DISK must be encoded as '%252F'
        force=0|1
        force_level_1=0|1
        force_bump=0|1
        force_no_bump=0|1

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

sub list {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Dles");
    my ($status, @result_messages) = Amanda::Rest::Configs::config_init(@_);
    return ($status, \@result_messages) if @result_messages;

    if (defined $params{'DISK'} and !defined $params{'disk'}) {
	$params{'disk'} = uri_unescape($params{'DISK'});
    }

    my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
    Amanda::Disklist::unload_disklist();
    my $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400006,
			severity     => $Amanda::Message::ERROR,
			diskfile     => $diskfile,
			cfgerr_level => $cfgerr_level);
	return (-1, \@result_messages);
    }

    my @hosts;
    if (defined $params{'HOST'}) {
	my $host = Amanda::Disklist::get_host($params{'HOST'});
	if (!$host) {
	    push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400007,
			severity     => $Amanda::Message::ERROR,
			diskfile     => $diskfile,
			host         => $params{'HOST'});
	    return (-1, \@result_messages);
	}
	@hosts = ( $host );
    } else {
	@hosts = Amanda::Disklist::all_hosts();
    }
    my @result;
    foreach my $host (@hosts) {
	my @disks;
	if (defined $params{'disk'}) {
	    my $disk = $host->get_disk($params{'disk'});
	    if (!$disk) {
		push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400008,
			severity     => $Amanda::Message::ERROR,
			diskfile     => $diskfile,
			host         => $host->{'hostname'},
			disk         => $params{'disk'});
		next;
	    }
	    @disks = ( $disk );
	} else {
	    @disks = $host->all_disks($host);
	}
	foreach my $disk (@disks) {

	    my $result = {
			host         => $host->{'hostname'},
			disk         => $disk->{'name'},
			device => $disk->{'device'},
			spindle => $disk->{'spindle'},
			dumptype_name => dumptype_name($disk->{'config'}),
		};
	    if ($params{'expand_dumptype'}) {
		$result->{'dumptype'} = Amanda::Rest::Configs::dumptype_object($disk->{'config'}, %params);
	    }

	    push @result, $result;
	}
    }
    if (@result) {
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400010,
			severity     => $Amanda::Message::INFO,
			diskfile     => $diskfile,
			result       => \@result);
    }
    return (-1, \@result_messages);
}

sub setting {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Dles");
    my ($status, @result_messages) = Amanda::Rest::Configs::config_init(@_);
    return ($status, \@result_messages) if @result_messages;

    if (defined $params{'DISK'} and !defined $params{'disk'}) {
	$params{'disk'} = uri_unescape($params{'DISK'});
    }

    if (!defined $params{'disk'}) {
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400009,
			severity     => $Amanda::Message::ERROR);
	return (404, \@result_messages);
    }

    my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
    Amanda::Disklist::unload_disklist();
    my $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400006,
			severity     => $Amanda::Message::ERROR,
			diskfile     => $diskfile,
			cfgerr_level => $cfgerr_level);
	return (-1, \@result_messages);
    }

    my $curinfodir = getconf($CNF_INFOFILE);;
    my $ci = Amanda::Curinfo->new($curinfodir);
    my $host = Amanda::Disklist::get_host($params{'HOST'});
    if (!$host) { # $host->isa("Amanda::Message");
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400007,
			severity     => $Amanda::Message::ERROR,
			diskfile     => $diskfile,
			host         => $params{'HOST'});
	return (-1, \@result_messages);
    }
    my @disks;
    if ($params{'disk'}) {
	my $disk = $host->get_disk($params{'disk'});
	if (!$disk) {  # $disk->isa("Amanda::Message");
	    push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400008,
			severity     => $Amanda::Message::ERROR,
			diskfile     => $diskfile,
			host         => $params{'HOST'},
			disk         => $params{'disk'});
	    return (-1, \@result_messages);
	}
	push @disks, $disk;
    } else {
	@disks = $host->all_disks();
    }

    if (!defined $params{'force'} and
	!defined $params{'force_level_1'} and
	!defined $params{'force_bump'} and
	!defined $params{'force_no_bump'}) {
	push @result_messages, Amanda::Curinfo::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1300030,
			severity     => $Amanda::Message::ERROR);
	return (-1, \@result_messages);
    }

    my $a = 0;
    $a++ if defined $params{'force'} and $params{'force'} == 1;
    $a++ if defined $params{'force_level_1'} and $params{'force_level_1'} == 1;
    $a++ if defined $params{'force_bump'} and $params{'force_bump'} == 1;
    $a++ if defined $params{'force_no_bump'} and $params{'force_no_bump'} == 1;
    if ($a > 1) {
	push @result_messages, Amanda::Curinfo::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1300031,
			severity     => $Amanda::Message::ERROR);
	return (-1, \@result_messages);
    }

    # remove setting
    for my $disk (@disks) {
	if ((defined $params{'force'}         and !$params{'force'}) or
	    (defined $params{'force_level_1'} and !$params{'force_level_1'})) {
	    push @result_messages, $ci->unforce($disk);
	}
	if ((defined $params{'force_bump'}    and !$params{'force_bump'}) or
	    (defined $params{'force_no_bump'} and !$params{'force_no_bump'})) {
	    push @result_messages, $ci->unforce_bump($disk);
	}
    }

    # add new setting
    for my $disk (@disks) {
	if (defined $params{'force'}) {
	    if ($params{'force'}) {
		push @result_messages, $ci->force($disk);
	    }
	}
	if (defined $params{'force_level_1'}) {
	    if ($params{'force_level_1'}) {
		push @result_messages, $ci->force_level_1($disk);
	    }
	}
	if (defined $params{'force_bump'}) {
	    if ($params{'force_bump'}) {
		push @result_messages, $ci->force_bump($disk);
	    }
	}
	if (defined $params{'force_no_bump'}) {
	    if ($params{'force_no_bump'}) {
		push @result_messages, $ci->force_no_bump($disk);
	    }
	}
    }
    return ($status, \@result_messages);
}

sub info {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Dles");
    my ($status, @result_messages) = Amanda::Rest::Configs::config_init(@_);
    return ($status, \@result_messages) if @result_messages;

    if (defined $params{'DISK'} and !defined $params{'disk'}) {
	$params{'disk'} = uri_unescape($params{'DISK'});
    }

    if (!defined $params{'disk'}) {
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400009,
			severity     => $Amanda::Message::ERROR);
	return (404, \@result_messages);
    }

    my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
    Amanda::Disklist::unload_disklist();
    my $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400006,
			severity     => $Amanda::Message::ERROR,
			diskfile     => $diskfile,
			cfgerr_level => $cfgerr_level);
	return (-1, \@result_messages);
    }

    my $curinfodir = getconf($CNF_INFOFILE);;
    my $ci = Amanda::Curinfo->new($curinfodir);
    my $host = Amanda::Disklist::get_host($params{'HOST'});
    if (!$host) { # $host->isa("Amanda::Message");
	push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400007,
			severity     => $Amanda::Message::ERROR,
			diskfile     => $diskfile,
			host         => $params{'HOST'});
	return (-1, \@result_messages);
    }
    my @dles;
    if ($params{'disk'}) {
	my $disk = $host->get_disk($params{'disk'});
	if (!$disk) {  # $disk->isa("Amanda::Message");
	    push @result_messages, Amanda::Disklist::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1400008,
			severity     => $Amanda::Message::ERROR,
			diskfile     => $diskfile,
			host         => $params{'HOST'},
			disk         => $params{'disk'});
	    return (-1, \@result_messages);
	}
	push @dles, $disk;
    } else {
	@dles = $host->all_disks();
    }

    for my $dle (@dles) {
	my $info = $ci->get_dle_info($dle);
	return $info if $info->isa("Amanda::Message");
	$info->{'force-full'}    = 1 if $info->isset($Amanda::Curinfo::Info::FORCE_FULL);
	$info->{'force-level-1'} = 1 if $info->isset($Amanda::Curinfo::Info::FORCE_LEVEL_1);
	$info->{'force-bump'}    = 1 if $info->isset($Amanda::Curinfo::Info::FORCE_BUMP);
	$info->{'force-no-bump'}    = 1 if $info->isset($Amanda::Curinfo::Info::FORCE_NO_BUMP);
	push @result_messages, Amanda::Curinfo::Message->new(
			source_filename => __FILE__,
			source_line     => __LINE__,
			code         => 1300033,
			severity     => $Amanda::Message::SUCCESS,
			host         => $dle->{'host'}->{'hostname'},
			disk         => $dle->{'name'},
			info	     => $info);
    }
    return ($status, \@result_messages);
}

1;
