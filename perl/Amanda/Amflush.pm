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

package Amanda::Amflush::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 2200000) {
	return "The log file is '$self->{'logfile'}'";
    } elsif ($self->{'code'} == 2200001) {
	return "The amdump trace file is '$self->{'tracefile'}'";
    } elsif ($self->{'code'} == 2200002) {
	return "The Datestamp '$self->{'datestamp'} doesn't match a holding disk datestamps";
    } elsif ($self->{'code'} == 2200003) {
	return "None of the datestamp provided match a holding datestamp."
    } elsif ($self->{'code'} == 2200004) {
	return "Nothing to flush";
    } elsif ($self->{'code'} == 2200005) {
	return "Running a flush";
    } elsif ($self->{'code'} == 2200006) {
	return "The timestamp is '$self->{'timestamp'}'";
    }
}


package Amanda::Amflush;
use strict;
use warnings;

use POSIX qw(WIFEXITED WEXITSTATUS strftime);
use File::Glob qw( :glob );

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants :quoting );
use Amanda::Logfile qw( :logtype_t log_add );
use Amanda::Debug qw( debug );
use Amanda::Paths;
use Amanda::Holding;
use Amanda::Cmdfile;

sub new {
    my $class = shift @_;
    my %params = @_;

    my @result_messages;

    my $self = \%params;
    bless $self, $class;

    my $logdir = $self->{'logdir'} = config_dir_relative(getconf($CNF_LOGDIR));
    my @now = localtime;
    $self->{'longdate'} = strftime "%a %b %e %H:%M:%S %Z %Y", @now;

    my $timestamp = strftime "%Y%m%d%H%M%S", @now;
    $self->{'timestamp'} = Amanda::Logfile::make_logname("amflush", $timestamp);
    $self->{'trace_log_filename'} = Amanda::Logfile::get_logname();
    $self->{'pid'} = $$;
    debug("beginning trace log: $self->{'trace_log_filename'}");

    $timestamp = $self->{'timestamp'};
    $self->{'datestamp'} = strftime "%Y%m%d", @now;
    $self->{'starttime_locale_independent'} = strftime "%Y-%m-%d %H:%M:%S %Z", @now;
    $self->{'amdump_log_pathname_default'} = "$logdir/amdump";
    $self->{'amdump_log_pathname'} = "$logdir/amdump.$timestamp";
    $self->{'tracefile_path'} = "$logdir/amdump.$timestamp";
    $self->{'amdump_log_filename'} = "amdump.$timestamp";
    $self->{'tarcefile'} = "amdump.$timestamp";
    $self->{'exit_code'} = 0;
    $self->{'amlibexecdir'} = 0;

    debug("beginning amdump log");
    # Must be opened in append so that all subprocess can write to it.
    open($self->{'amdump_log'}, ">>", $self->{'amdump_log_pathname'})
	or die("could not open amdump log file '$self->{'amdump_log_pathname'}': $!");
    unlink $self->{'amdump_log_pathname_default'};
    symlink $self->{'amdump_log_filename'}, $self->{'amdump_log_pathname_default'};
    push @result_messages, Amanda::Amflush::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 2200006,
			severity    => $Amanda::Message::INFO,
			timestamp   => $self->{'timestamp'});
    push @result_messages, Amanda::Amflush::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 2200001,
			severity    => $Amanda::Message::INFO,
			tracefile   => $self->{'amdump_log_pathname'});
    push @result_messages, Amanda::Amflush::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 2200000,
			severity    => $Amanda::Message::INFO,
			logfile     => $self->{'trace_log_filename'});
    return $self, \@result_messages;
}

sub user_msg {
    my $self = shift;
    my $msg = shift;

    if (defined $self->{'user_msg'}) {
	$self->{'user_msg'}->($msg);
    }
}

##
# subs for below

sub amdump_log {
    my $self = shift;

    print {$self->{'amdump_log'}} "amflush: ", @_, "\n";
}

sub check_exec {
    my $self = shift;

    my ($prog) = @_;
    return if -x $prog;

    log_add($L_ERROR, "Can't execute $prog");
}

sub run_subprocess {
    my $self = shift;

    my ($proc, @args) = @_;
    $self->check_exec($proc);

    debug("Running $proc " . join(' ', @args));
    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $null = POSIX::open("/dev/null", POSIX::O_RDWR);
	POSIX::dup2($null, 0);
	POSIX::dup2($null, 1);
	POSIX::dup2(fileno($self->{'amdump_log'}), 2);
	close($self->{'amdump_log'});
	exec $proc, @args;
	die "Could not exec $proc: $!";
    }
    waitpid($pid, 0);
    my $s = $? >> 8;
    debug("$proc exited with code $s");
    if ($?) {
	$self->{'exit_code'} |= $s;
    }
}

sub wait_for_hold {
    my $self = shift;

    my $holdfile = "$CONFIG_DIR/$self->{'config'}/hold";
    if (-f $holdfile) {
	debug("waiting for hold file '$holdfile' to be removed");
	while (-f $holdfile) {
	    sleep(60);
	}
    }
}

sub do_amreport {
    my $self = shift;

    debug("running amreport");
    $self->run_subprocess("$sbindir/amreport", $self->{'config'}, '--from-amdump', '-l',
		   $self->{'trace_log_filename'}, @{$self->{'config_overrides'}});
}

sub roll_amdump_logs {
    my $self = shift;

    debug("renaming amdump log and trimming old amdump logs (beyond tapecycle+2)");

    unlink "$self->{'amdump_log_pathname_default'}.1";
    rename $self->{'amdump_log_pathname_default'}, "$self->{'amdump_log_pathname_default'}.1";

    # keep the latest tapecycle files.
    my $logdir = $self->{'logdir'};
    my @files = sort {-M $b <=> -M $a} grep { !/^\./ && -f "$_"} <$logdir/amdump.*>;
    my $days = getconf($CNF_TAPECYCLE) + 2;
    for (my $i = $days-1; $i >= 1; $i--) {
	my $a = pop @files;
    }
    foreach my $name (@files) {
	unlink $name;
	$self->amdump_log("unlink $name");
    }
}

my $ctrl_c = 0;
sub _interrupt {
    $ctrl_c = 1;
}

sub get_flush {
    my $self = shift;
    my %params = @_;

    my @to_flush;
    my @ts;
    if (!$params{'datestamps'}) {
	@ts = Amanda::Holding::get_all_datestamps();
    } else {
	@ts = @{$params{'datestamps'}};
    }
    @ts = sort @ts;
    my @hfiles = Amanda::Holding::get_files_for_flush(@ts);

    my $conf_cmdfile = config_dir_relative(getconf($CNF_CMDFILE));
    my $cmdfile = Amanda::Cmdfile->new($conf_cmdfile);
    my $cmd_added;

    my %disks;
    for my $hfile (@hfiles) {
	my $hdr = Amanda::Holding::get_header($hfile);
	my $dle = Amanda::Disklist::get_disk($hdr->{'name'}, $hdr->{'disk'});
	next if !$dle->{'todo'};
	if (!defined $disks{$hdr->{'name'}}{$hdr->{'disk'}}) {
	    log_add($L_DISK, $hdr->{'name'} . " " . quote_string($hdr->{'disk'}));
	    $disks{$hdr->{'name'}}{$hdr->{'disk'}} = 1;
	}
	my $ids = $cmdfile->get_ids_for_holding($hfile);
	if (!$ids) {
	    my $storages = getconf($CNF_STORAGE);
	    my $storage_name = $storages->[0];
	    my $cmddata = Amanda::Cmdfile->new_Cmddata(
		operation      => $Amanda::Cmdfile::CMD_FLUSH,
		config         => get_config_name(),
		holding_file   => $hfile,
		hostname       => $hdr->{'name'},
		diskname       => $hdr->{'disk'},
		dump_timestamp => $hdr->{'datestamp'},
		dst_storage    => $storage_name,
		working_pid    => $$,
		status         => $Amanda::Cmdfile::CMD_TODO);
	    my $id = $cmdfile->add_to_memory($cmddata);
	    $ids = "$id;$storage_name";
	    $cmd_added++;
	}

	my $to_flush = { hfile     => $hfile,
			 host      => $hdr->{'name'},
			 disk      => $hdr->{'disk'},
			 datestamp => $hdr->{'datestamp'},
			 dumplevel => $hdr->{'dumplevel'},
			 ids       => $ids};
	push @to_flush, $to_flush;
    }
    if ($cmd_added) {
	$cmdfile->write();
    } else {
	$cmdfile->unlock();
    }

    return \@to_flush;
}

sub run {
    my $self = shift;
    my $catch_ctrl_c = shift;
    my $to_flushs = shift;

    $self->{'pid'} = $$;

    # wait for $confdir/hold to disappear
    $self->wait_for_hold();

    if ($catch_ctrl_c) {
	$SIG{INT} = \&_interrupt;
    }

    # amstatus needs a lot of forms of the time, I guess
    $self->amdump_log("start at $self->{'longdate'}");
    $self->amdump_log("datestamp $self->{'datestamp'}");
    $self->amdump_log("starttime $self->{'timestamp'}");
    $self->amdump_log("starttime-locale-independent $self->{'starttime_locale_independent'}");

    log_add($L_START, "date $self->{'timestamp'}");
    #fork the driver
    my ($rpipe, $driver_pipe) = POSIX::pipe();
    my $driver_pid = POSIX::fork();
    if ($driver_pid == 0) {
	my $driver = "$amlibexecdir/driver";
	my @log_filename = ('--log-filename', $self->{'trace_log_filename'});
	my @config_overrides = ();
	if (defined $self->{'config_overrides'} and
	    @{$self->{'config_overrides'}}) {
	    @config_overrides = @{$self->{'config_overrides'}};
	}
	## child, exec the driver
	POSIX::dup2($rpipe, 0);
	POSIX::close($rpipe);
	POSIX::close($driver_pipe);
	POSIX::dup2(fileno($self->{'amdump_log'}), 1);
	POSIX::dup2(fileno($self->{'amdump_log'}), 2);
	debug("exec: " . join(' ', $driver, $self->{'config'}, "nodump", "--no-vault",
				   @log_filename, @config_overrides));
	close($self->{'amdump_log'});
	exec $driver, $self->{'config'}, "nodump", "--no-vault", @log_filename,
		      @config_overrides;
	die "Could not exec $driver: $!";
    }
    debug(" driver: $driver_pid");
    open my $driver_stream, ">&=$driver_pipe";
    POSIX::close($rpipe);

    print {$driver_stream} "DATE $self->{'timestamp'}\n";

    my %disks;
    foreach my $to_flush (@$to_flushs) {
	next if !defined $to_flush;
	if (!defined $disks{$to_flush->{'host'}} and
	    !defined $disks{$to_flush->{'host'}}{$to_flush->{'disk'}}) {
	    log_add($L_DISK, $to_flush->{'host'} . " " . quote_string($to_flush->{'disk'}));
	    $disks{$to_flush->{'host'}}{$to_flush->{'disk'}} = 1;
	}
	my $line =  "FLUSH $to_flush->{'ids'} $to_flush->{'host'} " .
				quote_string($to_flush->{'disk'}) .
				" $to_flush->{'datestamp'} " .
				"$to_flush->{'dumplevel'} " .
				quote_string($to_flush->{'hfile'});
	print STDERR "$line\n";
	print {$driver_stream} "$line\n";
	debug("flushing $to_flush->{'hfile'}");
    }

    print STDERR "ENDFLUSH\n";
    print {$driver_stream} "ENDFLUSH\n";
    close($driver_stream);

    my $dead = wait();
    die("Error waiting: $!") if ($dead <= 0);
    my $s = $? >> 8;
    debug("driver finished with exit code $s");
    my $exit = WIFEXITED($?)? WEXITSTATUS($?) : 1;
    $self->{'exit_code'} |= $exit if $exit;

    if ($catch_ctrl_c) {
	if ($ctrl_c == 1) {
	    print "Caught a ctrl-c\n";
	    log_add($L_FATAL, "amflush killed by ctrl-c");
	    debug("Caught a ctrl-c");
	    $self->{'exit_code'} = 1;
	}
	$SIG{INT} = 'DEFAULT';
    }

    my $end_longdate = strftime "%a %b %e %H:%M:%S %Z %Y", localtime;
    $self->amdump_log("end at $end_longdate");

    # send the dump report
    $self->do_amreport();

    # do some house-keeping
    $self->roll_amdump_logs();

    log_add($L_INFO, "pid-done $self->{'pid'}");
    debug("Amflush exiting with code $self->{'exit_code'}");
    return($self->{'exit_code'});
}

1;
