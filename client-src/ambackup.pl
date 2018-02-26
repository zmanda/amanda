#! @PERL@
# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
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

use lib '@amperldir@';
use strict;
use warnings;

use Getopt::Long;
use Symbol;
use IPC::Open3;
use Sys::Hostname;
use XML::Simple;
use JSON -convert_blessed_universally;

use Amanda::Util qw( :constants );
use Amanda::Config qw( :init :getconf );
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Util qw ( match_disk );
use Amanda::Debug qw( debug );
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Xfer qw( :constants );
use Amanda::Feature;

%ENV = ();

Amanda::Util::setup_application("ambackup", "client", $CONTEXT_CMDLINE, "amanda", "amanda");

my $config;
my $config_overrides = new_config_overrides($#ARGV+1);
my @config_overrides_opts;
my $opt_verbose = 0;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw{bundling});
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'config=s' => sub { $config = $_[1]; },
    'verbose' => sub { $opt_verbose++; },
    'o=s' => sub {
	push @config_overrides_opts, "-o" . $_[1];
	add_config_override_opt($config_overrides, $_[1]);
     },
) or usage();

if (@ARGV < 1) {
    die "USAGE: ambackup [--config <config>] [--verbose] <config-overwrites> [list|check|backup] <diskname>";
}

my $cmd = shift @ARGV;

set_config_overrides($config_overrides);
config_init($CONFIG_INIT_CLIENT, undef);
$config = getconf($CNF_CONF) if !defined $config;
config_init($CONFIG_INIT_CLIENT | $CONFIG_INIT_EXPLICIT_NAME | $CONFIG_INIT_OVERLAY, $config);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
        die "Errors processing config file";
    }
}

my $hostname = getconf($CNF_HOSTNAME);
if (!$hostname) {
    $hostname = Sys::Hostname::hostname;
}
#Amanda::Debug::add_amanda_log_handler($amanda_log_trace_log);

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $amservice = $sbindir . '/amservice';
my $selfcheck = $amlibexecdir . '/selfcheck';
my $sendbackup = $amlibexecdir . '/sendbackup';
my $amdump_server = getconf($CNF_AMDUMP_SERVER);
my $auth = getconf($CNF_AUTH);
my $their_features;

get_features();

my @disks;
if ($cmd eq 'list') {
    get_list(1);
} elsif ($cmd eq 'check') {
    get_list(0);
    my $rep = get_check(0);
    debug("rep: $rep");
    run_selfcheck($rep, $opt_verbose);
} elsif ($cmd eq 'backup') {
    get_list(0);
    if (@ARGV) {
	foreach my $diskname (@ARGV) {
	    get_backup($diskname, $opt_verbose);
	}
    } elsif (@disks) {
	foreach my $diskname (@disks) {
	    get_backup($diskname, $opt_verbose);
	}
    } else {
	print "No Dle to backup.\n";
    }
} else {
    print "Unknown '$cmd' command.\n";
}

sub get_features {
    my @cmd = ($amservice, @config_overrides_opts, "--config", $config, $amdump_server, $auth, 'noop');

    debug("cmd: " . join(' ',@cmd));
    my $amservice_out;
    my $amservice_in;
    my $err = Symbol::gensym;
    my $pid = open3($amservice_in, $amservice_out, $err, @cmd);

    close $amservice_in;

    while (<$amservice_out>) {
	next if $_ eq "\n";
	chomp;
	s/^OPTIONS features=//g;
	$their_features = Amanda::Feature::Set->from_string($_);
    }
    close($amservice_out);

    waitpid $pid, 0;
}

sub get_list
{
    my $verbose = shift;
    my @cmd = ($amservice, @config_overrides_opts, "--config", $config, $amdump_server, $auth, 'ambackupd');

    debug("cmd: " . join(' ',@cmd));
    my $amservice_out;
    my $amservice_in;
    my $err = Symbol::gensym;
    my $pid = open3($amservice_in, $amservice_out, $err, @cmd);

    debug("send: LISTDLE");
    print {$amservice_in} "LISTDLE\n";
    close $amservice_in;

    my $rep;
    while (<$amservice_out>) {
	$rep .= $_;
    }

    if ($rep =~ /^ERROR/) {
	print $rep;
    } else {
	my $json = JSON->new->allow_nonref->convert_blessed;
	my $reply;
	my $rv = eval { $reply = $json->decode($rep); };
	if ($@) {
	    my $err = $@;
	    print "ERROR: $err\n";
	} else {
	    foreach my $message (@$reply) {
		push @disks, $message->{'diskname'};
		print $message->{'message'},"\n" if $verbose;
	    }
	}
    }
    close($amservice_out);

    waitpid $pid, 0;
}

sub get_check
{
    my $verbose = shift;
    my @cmd = ($amservice, @config_overrides_opts, "--config", $config, $amdump_server, $auth, 'ambackupd');

    debug("cmd: @cmd");
    my $amservice_out;
    my $amservice_in;
    my $err = Symbol::gensym;
    my $pid = open3($amservice_in, $amservice_out, $err, @cmd);

    debug("send: CHECK");
    print {$amservice_in} "CHECK\n";
    close $amservice_in;

    my $rep;
    while (<$amservice_out>) {
	next if $_ eq "\n";
	print if $verbose;
	if (/^GOPTIONS/) {
	    $_ =~ s/^GOPTIONS/OPTIONS/;
	    # remove fe_selfcheck_message
	    /features=([^;]*);/;
	    my $feature_str = $1;
	    my $feature = Amanda::Feature::Set->from_string($feature_str);
	    $feature->remove($Amanda::Feature::fe_selfcheck_message);
	    my $new_feature_str = $feature->as_string();
	    $_ =~ s/$feature_str/$new_feature_str/;
	} elsif (/^ERROR/) {
	    print if !$verbose;
	}
	$rep .= $_;
    }
    close($amservice_out);

    waitpid $pid, 0;

    return $rep;
}

sub run_selfcheck {
    my $rep = shift;
    my $verbose = shift;

    my @cmd = ($selfcheck);

    debug("cmd: @cmd");
    my $selfcheck_out;
    my $selfcheck_in;
    my $err = Symbol::gensym;
    my $pid = open3($selfcheck_in, $selfcheck_out, $err, @cmd);

    print {$selfcheck_in} $rep;
    close $selfcheck_in;

    my $error = 0;
    while(<$selfcheck_out>) {
	next if /^OK/ && !$verbose;
	next if /^OPTIONS/ && !$verbose;
	$error++ if /^ERROR/;
	print;
    }
    close $selfcheck_out;

    while(<$err>) {
	$error++;
	print;
    }
    close $err;

    waitpid $pid, 0;

    if (!$error) {
	print "check succeeded\n";
    } else {
	print "check failed with $error errors\n";
    }
}

use Fcntl qw( F_GETFD F_SETFD FD_CLOEXEC );
use FileHandle;

sub get_backup
{
    my $diskname = shift;
    my $verbose = shift;

    printf("\nDoing backup of $diskname\n");
    my ($data_amservice_in, $data_ambackup_out) = FileHandle::pipe;	# output to amservice
    my ($data_ambackup_in, $data_amservice_out) = FileHandle::pipe;	# input from amservice
    my ($mesg_amservice_in, $mesg_ambackup_out) = FileHandle::pipe;	# output to amservice
    my ($mesg_ambackup_in, $mesg_amservice_out) = FileHandle::pipe;	# input from amservice
    my ($index_amservice_in, $index_ambackup_out) = FileHandle::pipe;	# output to amservice
    my ($index_ambackup_in, $index_amservice_out) = FileHandle::pipe;	# input from amservice
    my ($state_amservice_in, $state_ambackup_out) = FileHandle::pipe;	# output to amservice
    my ($state_ambackup_in, $state_amservice_out) = FileHandle::pipe;	# input from amservice

    # remove FD_CLOEXEC flag
    fcntl($data_amservice_in  , F_SETFD, fcntl($data_amservice_in  , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($data_amservice_out , F_SETFD, fcntl($data_amservice_out , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($mesg_amservice_in  , F_SETFD, fcntl($mesg_amservice_in  , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($mesg_amservice_out , F_SETFD, fcntl($mesg_amservice_out , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($index_amservice_in , F_SETFD, fcntl($index_amservice_in , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($index_amservice_out, F_SETFD, fcntl($index_amservice_out, F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    if ($their_features->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	fcntl($state_amservice_in , F_SETFD, fcntl($state_amservice_in , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
	fcntl($state_amservice_out, F_SETFD, fcntl($state_amservice_out, F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    }

    # remove ALL flags
    fcntl($data_amservice_in  , F_SETFD, 0);
    fcntl($data_amservice_out , F_SETFD, 0);
    fcntl($mesg_amservice_in  , F_SETFD, 0);
    fcntl($mesg_amservice_out , F_SETFD, 0);
    fcntl($index_amservice_in , F_SETFD, 0);
    fcntl($index_amservice_out, F_SETFD, 0);
    if ($their_features->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	fcntl($state_amservice_in , F_SETFD, 0);
	fcntl($state_amservice_out, F_SETFD, 0);
    }

    my @cmd = ($amservice, @config_overrides_opts, "--config", $config,
			"--stream", "DATA,".fileno($data_amservice_in).",".fileno($data_amservice_out),
			"--stream", "MESG,".fileno($mesg_amservice_in).",".fileno($mesg_amservice_out),
			"--stream", "INDEX,".fileno($index_amservice_in).",".fileno($index_amservice_out));
    if ($their_features->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	push @cmd, "--stream", "STATE,".fileno($state_amservice_in).",".fileno($state_amservice_out),
    }
    push @cmd,	$amdump_server, $auth, 'ambackupd';
    debug("cmd: @cmd");

    my $amservice_out;
    my $amservice_in;
    my $err = Symbol::gensym;
    my $pid = open3($amservice_in, $amservice_out, $err, @cmd);
    debug("send: BACKUP $diskname");
    print {$amservice_in} "BACKUP $diskname\n";
    close $amservice_in;

    close($data_amservice_in);
    close($data_amservice_out);
    close($mesg_amservice_in);
    close($mesg_amservice_out);
    close($index_amservice_in);
    close($index_amservice_out);
    if ($their_features->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	debug("closing amservice state pipe");
	close($state_amservice_in);
	close($state_amservice_out);
    }

    # we never read the data stream from amservice/ambackupd
    close($data_ambackup_in);
    # index stream is plugged directly to sendbackup
    # state stream is plugged directly to sendbackup
    # we will read mesg stream.

    my $rep;
    my $dle_str;
    while (<$amservice_out>) {
	next if $_ eq "\n";
	print if $verbose > 1;
	$_ =~ s/^GOPTIONS/OPTIONS/;
	if (/^ERROR/) {
	    print if !$verbose;
	}
	next if /^CONNECT/;
	$rep .= $_;
	if ($dle_str || /^<dle>/) {
	    $dle_str .= $_;
	}
    }
    close($amservice_out);

    debug("rep: $rep");

    my ($sdata_sendbackup_in , $sdata_ambackup_out) = FileHandle::pipe;
    my ($sdata_ambackup_in , $sdata_sendbackup_out) = FileHandle::pipe;
    my ($smesg_sendbackup_in , $smesg_ambackup_out) = FileHandle::pipe;
    my ($smesg_ambackup_in , $smesg_sendbackup_out) = FileHandle::pipe;

    # remove FD_CLOEXEC flag
    fcntl($sdata_sendbackup_in  , F_SETFD, fcntl($sdata_sendbackup_in  , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($sdata_sendbackup_out , F_SETFD, fcntl($sdata_sendbackup_out , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($smesg_sendbackup_in  , F_SETFD, fcntl($smesg_sendbackup_in  , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($smesg_sendbackup_out , F_SETFD, fcntl($smesg_sendbackup_out , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($index_ambackup_in , F_SETFD, fcntl($index_ambackup_in , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    fcntl($index_ambackup_out, F_SETFD, fcntl($index_ambackup_out, F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    if ($their_features->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	fcntl($state_ambackup_in , F_SETFD, fcntl($state_ambackup_in , F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
	fcntl($state_ambackup_out, F_SETFD, fcntl($state_ambackup_out, F_GETFD, 0) & ~FD_CLOEXEC) or debug("Can't set");
    }

    POSIX::dup2(fileno($sdata_sendbackup_in) , $Amanda::Constants::DATA_FD_OFFSET + 1) || die;
    POSIX::dup2(fileno($sdata_sendbackup_out), $Amanda::Constants::DATA_FD_OFFSET + 0) || die;
    POSIX::dup2(fileno($smesg_sendbackup_in) , $Amanda::Constants::DATA_FD_OFFSET + 3) || die;
    POSIX::dup2(fileno($smesg_sendbackup_out), $Amanda::Constants::DATA_FD_OFFSET + 2) || die;
    POSIX::dup2(fileno($index_ambackup_in) , $Amanda::Constants::DATA_FD_OFFSET + 5) || die;
    POSIX::dup2(fileno($index_ambackup_out), $Amanda::Constants::DATA_FD_OFFSET + 4) || die;
    if ($their_features->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	POSIX::dup2(fileno($state_ambackup_in) , $Amanda::Constants::DATA_FD_OFFSET + 7) || die;
	POSIX::dup2(fileno($state_ambackup_out), $Amanda::Constants::DATA_FD_OFFSET + 6) || die;
    }

    close($sdata_sendbackup_in);
    close($sdata_sendbackup_out);
    close($smesg_sendbackup_in);
    close($smesg_sendbackup_out);
    close($index_ambackup_in);
    close($index_ambackup_out);
    if ($their_features->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	debug("closing state pipe");
	close($state_ambackup_in);
	close($state_ambackup_out);
    }

    my @cmd_sendbackup = ("$sendbackup");

    my $sendbackup_out;
    my $sendbackup_in;
    my $sendbackup_err = Symbol::gensym;
    debug("exec " . join(' ', @cmd_sendbackup));
    my $sendbackup_pid = open3($sendbackup_in, $sendbackup_out, $sendbackup_err,
			       @cmd_sendbackup);

    print {$sendbackup_in} $rep;
    close $$sendbackup_in;

    while (<$sendbackup_out>) {
	debug("sendbackup_out: $_");
	if (/^CONNECT/) {
	    # TODO: verify 3 of 4 stream
	}
    }

    POSIX::close($Amanda::Constants::DATA_FD_OFFSET + 0);
    POSIX::close($Amanda::Constants::DATA_FD_OFFSET + 1);
    POSIX::close($Amanda::Constants::DATA_FD_OFFSET + 2);
    POSIX::close($Amanda::Constants::DATA_FD_OFFSET + 3);
    POSIX::close($Amanda::Constants::DATA_FD_OFFSET + 4);
    POSIX::close($Amanda::Constants::DATA_FD_OFFSET + 5);
    if ($their_features->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	POSIX::close($Amanda::Constants::DATA_FD_OFFSET + 6);
	POSIX::close($Amanda::Constants::DATA_FD_OFFSET + 7);
    }

    my $file_to_close = 0;

    my @sdata_link;
    my $sdata_src  = Amanda::Xfer::Source::Fd->new($sdata_ambackup_in);
    close($sdata_ambackup_in);
    push @sdata_link, $sdata_src;

    my $sdata_dest = Amanda::Xfer::Dest::Fd->new($data_ambackup_out);
    close($data_ambackup_out);
    push @sdata_link, $sdata_dest;
    my $xfer_data = Amanda::Xfer->new(\@sdata_link);
    $file_to_close++;
    $xfer_data->start(sub {
	my ($src, $xmsg, $xfer) = @_;
	debug("Message from data $xfer: $xmsg"); # use stringify operations
	if ($xmsg->{'type'} == $XMSG_DONE) {
	    $file_to_close--;
	    Amanda::MainLoop::quit() if $file_to_close == 0;
	}
    }, 0, 0);

    my $smesg_src  = Amanda::MainLoop::fd_source($smesg_ambackup_in , $G_IO_IN|$G_IO_HUP|$G_IO_ERR);

    $file_to_close++;
    $smesg_src->set_callback( sub {
	my $buf;
	my $blocksize = sysread($smesg_ambackup_in, $buf, 32768);
	debug("sread mesg: $blocksize");
	if (!$blocksize) {
	    $file_to_close--;
	    $smesg_src->remove();
	    close($smesg_ambackup_in);
	} else {
	    syswrite($mesg_ambackup_out, $buf, $blocksize);
	    debug("sendbackup mesg: $buf");
	    if ($verbose) {
		print STDOUT $buf;
	    }
	}
    });

    my $mesg_src  = Amanda::MainLoop::fd_source($mesg_ambackup_in , $G_IO_IN|$G_IO_HUP|$G_IO_ERR);

    $file_to_close++;
    $mesg_src->set_callback( sub {
	my $buf;
	my $blocksize = sysread($mesg_ambackup_in, $buf, 32768);
	debug("read mesg: $blocksize");
	if (!$blocksize) {
	    $file_to_close--;
	    $mesg_src->remove();
	    $smesg_src->remove();
	    close($mesg_ambackup_in);
	    Amanda::MainLoop::quit();
	} else {
	    my @lines = split "\n", $buf;
	    for my $line (@lines) {
		if ($line eq "MESG END") {
		    $file_to_close--;
		    $mesg_src->remove();
		    $smesg_src->remove();
		    close($mesg_ambackup_in);
		    Amanda::MainLoop::quit();
		} else {
		    debug("server mesg: $line");
		    print STDOUT "$line\n";
		}
	    }
	}
    });

    # we never write to data/mesg stream of sendbackup
    close($sdata_ambackup_out);
    close($smesg_ambackup_out);
    # we never write to index/state stream of amservice (sendbackup do it directly)
    close($index_ambackup_out);
    if ($their_features->has($Amanda::Feature::fe_sendbackup_stream_state)) {
	close($state_ambackup_out);
    }

    Amanda::MainLoop::run();

    close($err);
    close($sendbackup_err);

#    waitpid $sendbackup_pid, 0;  #it should be killed on error
#    waitpid $pid, 0;   # this wait for the 30s timeout of amandad
    debug("DONE amservice");

}

sub usage {
    print STDERR "USAGE: ambackup [--config <config>] [--verbose] <config-overwrites> [list|check|backup] <diskname>";
}

Amanda::Util::finish_application();
exit;
