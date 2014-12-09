# Copyright (c) 2014-2014 Zmanda, Inc.  All Rights Reserved.
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


package Amanda::Cleanup::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 3400000) {
        return "$self->{'process_name'} Process is running at PID $self->{'pid'} for $self->{'config_name'} configuration";
    } elsif ($self->{'code'} == 3400001) {
	return "$self->{'nb_amanda_process'} Amanda processes were found running";
    } elsif ($self->{'code'} == 3400002) {
        return "$self->{'nb_processes'} processes failed to terminate";
    } elsif ($self->{'code'} == 3400003) {
        return "no unprocessed logfile to clean up";
    } elsif ($self->{'code'} == 3400004) {
        return "$self->{'errfile'} exists, renaming it";
    } elsif ($self->{'code'} == 3400005) {
        return "failed to execute $self->{'program'}: $self->{'errstr'}";
    } elsif ($self->{'code'} == 3400006) {
        return "$self->{'program'} died with signal $self->{'signal'}";
    } elsif ($self->{'code'} == 3400007) {
        return "$self->{'program'} exited with value $self->{'exit_status'}";
    } elsif ($self->{'code'} == 3400008) {
        return "$self->{'program'} stdout: $self->{'line'}";
    } elsif ($self->{'code'} == 3400009) {
        return "$self->{'program'} stderr: $self->{'line'}";
    } elsif ($self->{'code'} == 3400010) {
        return "no trace_log";
    } else {
	return "no message for code $self->{'code'}";
    }
}

package Amanda::Cleanup;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );;
use Amanda::Util qw( :constants );
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Process;
use Amanda::Logfile;
use Amanda::Holding;
use Amanda::Debug qw( debug );
use IPC::Open3;

sub new {
    my $class = shift;
    my %params = @_;

    my $self = \%params;
    bless $self, $class;

    return $self;
}

sub user_message {
    my $self = shift;
    my $message = shift;

    if (defined $self->{'user_message'}) {
	$self->{'user_message'}->($message);
    } else {
	push @{$self->{'result_messages'}}, $message;
    }
}
sub cleanup {
    my $self = shift;
    my %params = @_;

    $self->{'kill'}          = $params{'kill'}          if defined $params{'kill'};
    $self->{'process_alive'} = $params{'process_alive'} if defined $params{'process_alive'};
    $self->{'verbose'}       = $params{'verbose'}       if defined $params{'verbose'};
    $self->{'clean_holding'} = $params{'clean_holding'} if defined $params{'clean_holding'};
    $self->{'trace_log'}     = $params{'trace_log'}     if defined $params{'trace_log'};
    $self->{'user_message'}  = $params{'user_message'}  if defined $params{'user_message'};

    $self->{'logdir'} = config_dir_relative(getconf($CNF_LOGDIR));
    $self->{'logfile'} = "$self->{'logdir'}/log";
    $self->{'logfile'} = $self->{'trace_log'} if defined $self->{'trace_log'};
    $self->{'amreport'} = "$sbindir/amreport";
    $self->{'amtrmidx'} = "$amlibexecdir/amtrmidx";
    $self->{'amcleanupdisk'} = "$sbindir/amcleanupdisk";

    my $config_name = Amanda::Config::get_config_name();

    my $Amanda_process = Amanda::Process->new($self->{'verbose'}, sub {my $message=shift; $self->user_message($message); });
    $Amanda_process->load_ps_table();

    $Amanda_process->scan_log($self->{'logfile'});
    $Amanda_process->add_child();

    my $nb_amanda_process = $Amanda_process->count_process();
    if ($nb_amanda_process > 0) {
	if ($self->{'process_alive'}) {
	    return;
	} elsif (!$self->{'kill'}) {
	    $self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400000,
			severity	=> $Amanda::Message::INFO,
			process_name	=> $Amanda_process->{master_pname},
			pid		=> $Amanda_process->{master_pid},
			config_name	=> $config_name));
	    return $self->{'result_messages'};
	} else { #kill the processes
	    $self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400001,
			severity	=> $Amanda::Message::INFO,
			nb_amanda_process => $nb_amanda_process));
	    Amanda::Debug::debug("Killing amanda process");
	    $Amanda_process->kill_process("SIGTERM");
	    my $count = 5;
	    my $pp;
	    while ($count > 0) {
		$pp = $Amanda_process->process_running();
		if ($pp > 0) {
		    $count--;
		    sleep 1;
		} else {
		    $count = 0;
		}
	    }
	    if ($pp > 0) {
		$count = 5;
		$Amanda_process->kill_process("SIGKILL");
		while ($count > 0 and $pp > 0) {
		    $count--;
		    sleep 1;
		    $pp = $Amanda_process->process_running();
		}
	    }
	    ($pp, my $pids) = $Amanda_process->which_process_running();
	    $self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400002,
			severity	=> $pp == 0 ? $Amanda::Message::INFO : $Amanda::Message::ERROR,
			pids		=> $pids,
			nb_processes	=> $pp));
	}
    }

    # rotate log
    if (-f $self->{'logfile'}) {
	Amanda::Debug::debug("Processing log file");
	$self->run_system(0, $self->{'amreport'}, $config_name, "--from-amdump");

	my $ts = Amanda::Logfile::get_current_log_timestamp();
	Amanda::Logfile::log_rename($ts);

	$self->run_system(1, $self->{'amtrmidx'}, $config_name);
    } else {
	$self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400003,
			severity	=> $Amanda::Message::INFO));
    }

    my $tapecycle = getconf($CNF_TAPECYCLE);

    # cleanup logfiles
    chdir "$CONFIG_DIR/$config_name";
    foreach my $pname ("amdump", "amflush") {
	my $errfile = "$self->{'logdir'}/$pname";
	if (-f $errfile) {
	    $self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400004,
			severity	=> $Amanda::Message::INFO,
			errfile		=> $errfile));

	    # Keep debug log through the tapecycle plus a couple days
	    my $maxdays=$tapecycle + 2;

	    my $days=1;
	    # First, find out the last existing errfile,
	    # to avoid ``infinite'' loops if tapecycle is infinite
	    while ($days < $maxdays  && -f "$errfile.$days") {
		$days++;
	    }

	    # Now, renumber the existing log files
	    while ($days >= 2) {
		my $ndays = $days - 1;
		rename("$errfile.$ndays", "$errfile.$days");
		$days=$ndays;
	    }
	    rename($errfile, "$errfile.1");
	}
    }

    my @amcleanupdisk;
    push @amcleanupdisk, $self->{'amcleanupdisk'};
    push @amcleanupdisk, "-v" if $self->{'verbose'};
    push @amcleanupdisk, "-r" if $self->{'clean_holding'};
    push @amcleanupdisk, $config_name;
    $self->run_system(0, @amcleanupdisk);

    if (defined $self->{'result_messages'}) {
	return \@{$self->{'result_messages'}};
    } else {
	return;
    }
}

sub run_system {
    my $self = shift;
    my $check_code = shift;
    my @cmd = @_;
    my $program = $cmd[0];
    my @result_messages;

    debug("Running: " . join(' ', @cmd));
    my ($pid, $wtr, $rdr, $err);
    $err = Symbol::gensym;
    $pid = open3($wtr, $rdr, $err, @cmd);
    close $wtr;
    while (my $line = <$rdr>) {
	chomp;
	$self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400008,
			severity	=> $Amanda::Message::INFO,
			program		=> $program,
			line		=> $line));
    }
    close $rdr;
    while (my $line = <$err>) {
	chomp;
	$self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400009,
			severity	=> $Amanda::Message::ERROR,
			program		=> $program,
			line		=> $line));
    }
    close $err;
    waitpid $pid, 0;
    my $child_error = $?;
    my $errno = $!;

    if ($child_error == -1) {
	$self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400005,
			severity	=> $Amanda::Message::INFO,
			program		=> $program,
			errstr		=> $errno));
    } elsif ($child_error & 127) {
	$self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400006,
			severity	=> $Amanda::Message::INFO,
			program		=> $program,
			signal		=> ($child_error & 127)));
    } elsif ($check_code && $child_error > 0) {
	$self->user_message(Amanda::Cleanup::Message->new(
			source_filename	=> __FILE__,
			source_line	=> __LINE__,
			code		=> 3400007,
			severity	=> $Amanda::Message::INFO,
			program		=> $program,
			exit_status	=> ($child_error >> 8)));
    }
}

1;
