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

package Amanda::Amdump::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 2000000) {
	return "The log file is '$self->{'logfile'}'";
    } elsif ($self->{'code'} == 2000001) {
	return "The amdump trace file is '$self->{'tracefile'}'";
    } elsif ($self->{'code'} == 2000002) {
	return "Running a dump";
    } elsif ($self->{'code'} == 2000003) {
	return "The timestamp is '$self->{'timestamp'}'";
    } elsif ($self->{'code'} == 2000004) {
	return "one run";
    }
}


package Amanda::Amdump;
use strict;
use warnings;

use Getopt::Long;
use POSIX qw(WIFEXITED WEXITSTATUS strftime);
use File::Glob qw( :glob );
use File::Basename;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Logfile qw( :logtype_t log_add );
use Amanda::Debug qw( debug );
use Amanda::Paths;

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
    $self->{'timestamp'} = Amanda::Logfile::make_logname("amdump", $timestamp);
    $self->{'trace_log_filename'} = Amanda::Logfile::get_logname();
    debug("beginning trace log: $self->{'trace_log_filename'}");

    $timestamp = $self->{'timestamp'};
    $self->{'datestamp'} = strftime "%Y%m%d", @now;
    $self->{'starttime_locale_independent'} = strftime "%Y-%m-%d %H:%M:%S %Z", @now;
    $self->{'amdump_log_pathname_default'} = "$logdir/amdump";
    $self->{'amdump_log_pathname'} = "$logdir/amdump.$timestamp";
    $self->{'amdump_log_filename'} = "amdump.$timestamp";
    $self->{'exit_code'} = 0;
    $self->{'amlibexecdir'} = 0;
    $self->{'pid'} = $$;

    debug("beginning amdump log");
    # Must be opened in append so that all subprocess can write to it.
    open($self->{'amdump_log'}, ">>", $self->{'amdump_log_pathname'})
	or die("could not open amdump log file '$self->{'amdump_log_pathname'}': $!");
    unlink $self->{'amdump_log_pathname_default'};
    symlink $self->{'amdump_log_filename'}, $self->{'amdump_log_pathname_default'};
    push @result_messages, Amanda::Amdump::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 2000003,
			severity    => $Amanda::Message::INFO,
			timestamp   => $timestamp);
    push @result_messages, Amanda::Amdump::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 2000001,
			severity    => $Amanda::Message::INFO,
			tracefile   => $self->{'amdump_log_pathname'});
    push @result_messages, Amanda::Amdump::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code        => 2000000,
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

    print {$self->{'amdump_log'}} "amdump: ", @_, "\n";
}

sub check_exec {
    my $self = shift;

    my ($prog) = @_;
    return if -x $prog;

    if (!-f $prog) {
	log_add($L_ERROR, "Can't execute $prog: $!");
    } else {
	log_add($L_ERROR, "Can't execute $prog: is not executable");
    }
}

sub run_subprocess {
    my $self = shift;

    my ($proc, @args) = @_;
    $self->check_exec($proc);

    my ($rpipe, $wpipe) = POSIX::pipe();

    debug("Running $proc " . join(' ', @args));
    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $null = POSIX::open("/dev/null", POSIX::O_RDWR);
	POSIX::dup2($null, 0);
	POSIX::dup2($null, 1);
	POSIX::dup2($wpipe, 2);
	close($wpipe);
	close($rpipe);
	close($self->{'amdump_log'});
	exec $proc, @args;
	#log_add($L_ERROR, "Could not exec $proc: $!");
	die "Could not exec $proc: $!";
    }
    POSIX::close($wpipe);

    my $pname = Amanda::Util::get_pname();
    my $proc_name = basename $proc;
    open (my $stderr_fh, "<&=", $rpipe);
    while (<$stderr_fh>) {
	Amanda::Util::set_pname($proc_name);
	log_add($L_ERROR, "$_");
	Amanda::Util::set_pname($pname);
    }
    close($stderr_fh);

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

sub planner_driver_pipeline {
    my $self = shift;

    my $planner = "$amlibexecdir/planner";
    my $driver = "$amlibexecdir/driver";
    my @no_taper = $self->{'no_taper'} ? ('--no-taper'):();
    my @from_client = $self->{'from_client'} ? ('--from-client'):();
    my @exact_match = $self->{'exact_match'} ? ('--exact-match'):();
    my @log_filename = ('--log-filename', $self->{'trace_log_filename'});
    my @config_overrides = ();
    if (defined $self->{'config_overrides'} and
	@{$self->{'config_overrides'}}) {
	@config_overrides = @{$self->{'config_overrides'}};
    }

    my @hostdisk = ();
    if (defined $self->{'hostdisk'} and
	@{$self->{'hostdisk'}}) {
	@hostdisk = @{$self->{'hostdisk'}};
    }

    $self->check_exec($planner);
    $self->check_exec($driver);

    # Perl's open3 is an embarassment to the language.  We'll do this manually.
    debug("invoking planner | driver");
    my ($rpipe, $wpipe) = POSIX::pipe();

    my $pl_pid = POSIX::fork();
    if ($pl_pid == 0) {
	## child
	my $null = POSIX::open("/dev/null", POSIX::O_RDWR);
	POSIX::dup2($null, 0);
	POSIX::close($null);
	POSIX::dup2($wpipe, 1);
	POSIX::close($rpipe);
	POSIX::close($wpipe);
	POSIX::dup2(fileno($self->{'amdump_log'}), 2);
	debug("exec: " .join(' ', $planner, $self->{'config'}, '--starttime', $self->{'timestamp'}, @log_filename, @no_taper, @from_client, @exact_match, @config_overrides, @hostdisk));
	close($self->{'amdump_log'});
	# note that @no_taper must follow --starttime
	my @args = ($self->{'config'}, '--starttime', $self->{'timestamp'}, @log_filename, @no_taper, @from_client, @exact_match, @config_overrides, @hostdisk);
	debug("exec $planner " . join(' ', @args));
	exec $planner, @args;
	die "Could not exec $planner: $!";
    }
    debug(" planner: $pl_pid");

    my $dr_pid = POSIX::fork();
    if ($dr_pid == 0) {
	## child
	my $null = POSIX::open("/dev/null", POSIX::O_RDWR);
	POSIX::dup2($rpipe, 0);
	POSIX::close($rpipe);
	POSIX::close($wpipe);
	POSIX::dup2(fileno($self->{'amdump_log'}), 1); # driver does lots of logging to stdout..
	POSIX::close($null);
	POSIX::dup2(fileno($self->{'amdump_log'}), 2);
	debug("exec: " . join(' ', $driver, $self->{'config'}, @log_filename, @no_taper, @from_client, @config_overrides));
	close($self->{'amdump_log'});
	my @args = ($self->{'config'}, @log_filename, @no_taper, @from_client, @config_overrides);
	debug("exec $driver " . join(' ', @args));
	exec $driver, @args;
	die "Could not exec $driver: $!";
    }
    debug(" driver: $dr_pid");

    POSIX::close($rpipe);
    POSIX::close($wpipe);

    my $first_bad_exit = 0;
    for (my $i = 0; $i < 2; $i++) {
	my $dead = wait();
	die("Error waiting: $!") if ($dead <= 0);
	my $s = $? >> 8;
	debug("planner finished with exit code $s") if $dead == $pl_pid;
	debug("driver finished with exit code $s") if $dead == $dr_pid;
	my $exit = WIFEXITED($?)? WEXITSTATUS($?) : 1;
	$first_bad_exit = $exit if ($exit && !$first_bad_exit)
    }
    $self->{'exit_code'} |= $first_bad_exit;
}

sub do_amreport {
    my $self = shift;

    debug("running amreport");
    $self->run_subprocess("$sbindir/amreport", $self->{'config'}, '--from-amdump', '-l',
		   $self->{'trace_log_filename'}, @{$self->{'config_overrides'}});
}

sub trim_trace_logs {
    my $self = shift;

    debug("trimming old trace logs");
    $self->run_subprocess("$amlibexecdir/amtrmlog", $self->{'config'}, @{$self->{'config_overrides'}});
}

sub trim_indexes {
    my $self = shift;

    debug("trimming old indexes");
    $self->run_subprocess("$amlibexecdir/amtrmidx", $self->{'config'}, @{$self->{'config_overrides'}});
}

sub roll_amdump_logs {
    my $self = shift;

    debug("renaming amdump log and trimming old amdump logs (beyond tapecycle+2)");

    unlink "$self->{'amdump_log_pathname_default'}.1";
    rename $self->{'amdump_log_pathname_default'}, "$self->{'amdump_log_pathname_default'}.1";

    my $logdir = $self->{'logdir'};
    my @files = grep { !/^\./ && -f "$_"} <$logdir/amdump.*>;
    foreach my $file (@files) {
	my $log = $file;
	$log =~ s/amdump/log/;
	$log .= ".0";
	if ( -M $file > 30 and !-f $log) {
	    unlink $file;
	    debug("unlink $file");
	}
    }
}

my $ctrl_c = 0;
sub _interrupt {
    $ctrl_c = 1;
}

sub run {
    my $self = shift;
    my $catch_ctrl_c = shift;

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

    # run the planner and driver, the one piped to the other
    $self->planner_driver_pipeline();

    if ($catch_ctrl_c) {
	if ($ctrl_c == 1) {
	    print "Caught a ctrl-c\n";
	    log_add($L_FATAL, "amdump killed by ctrl-c");
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
    $self->trim_trace_logs();
    $self->trim_indexes();
    $self->roll_amdump_logs();

    log_add($L_INFO, "pid-done $self->{'pid'}");
    debug("Amdump exiting with code $self->{'exit_code'}");
    return($self->{'exit_code'});
}

1;
