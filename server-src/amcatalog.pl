#! @PERL@
# Copyright (c) 2011 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
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

use lib '@amperldir@';
use strict;
use warnings;

use File::Basename;
use Getopt::Long;
use Text::Wrap;

use Amanda::Debug qw( :logging );
use Amanda::Logfile;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Disklist;
use Amanda::Util qw( :constants :quoting match_disk match_host match_datestamp split_quoted_strings );
use Amanda::MainLoop;
use Amanda::Cmdfile;
use Amanda::DB::Catalog2;

my $exit_status = 0;

##
# Subcommand handling

my %subcommands;

sub usage {
    print STDERR "Usage: amcatalog [--interactive] [--version] [-o configoption]* <conf> <command> {<args>} ...\n";
    print STDERR "    Valid <command>s are:\n";
    print STDERR "        create                          # create the database\n";
    print STDERR "        upgrade                         # upgrade the database\n";
    print STDERR "        retention                       # recompute the retention\n";
    print STDERR "        validate                        # validate the database\n";
    print STDERR "        clean                           # clean the database\n";
    print STDERR "        export <filename>               # export the database to filename\n";
    print STDERR "        import <filename>               # import the database from filename\n";
#    print STDERR "        merge                           # merge the database with the log files\n";
    print STDERR "        dump [--all-configs] [--exact-match] [host [disk [date [...]]]] # list dump\n";
    print STDERR "        part [--all-configs] [--exact-match] [host [disk [date [...]]]] # list part\n";
    exit(1);
}

Amanda::Util::setup_application("amcatalog", "server", $CONTEXT_CMDLINE, "amanda", "amanda");

my $config_overrides = new_config_overrides($#ARGV+1);
my ($opt_config, $opt_command);
my $opt_all_configs;
my $opt_exact_match;
my $opt_interactive;
my $opt_timestamp;
my $opt_remove_pool;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'help|usage|?' => \&usage,
    'all-configs' => \$opt_all_configs,
    'exact-match' => \$opt_exact_match,
    'interactive' => \$opt_interactive,
    'timestamp'   => \$opt_timestamp,
    'remove-pool' => \$opt_remove_pool,
    'o=s'        => sub { add_config_override_opt($config_overrides, $_[1]); },
    'version'    => \&Amanda::Util::version_opt,
) or usage();

usage() if @ARGV == 0 && !$opt_interactive;
$opt_config = $ARGV[0];

set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, $opt_config);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	print STDERR "errors processing config file";
	exit 1;
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
$cfgerr_level += Amanda::Disklist::read_disklist('filename' => $diskfile);
($cfgerr_level < $CFGERR_ERRORS) || die "Errors processing disklist";

sub _out {
    my $catalog = shift;
    my $dbh = $catalog->{'dbh'};

    my $sth_host = $dbh->prepare("SELECT * FROM host")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_host->execute() or die "Cannot execute: " . sth_host->errstr();

    print "HOST:\n";
    my $all_row_host = $sth_host->fetchall_arrayref();
    for my $row_host (@{$all_row_host}) {
	print " $row_host->[0] $row_host->[1]\n";
    }

    my $sth_dle = $dbh->prepare("SELECT * FROM dle")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_dle->execute() or die "Cannot execute: " . sth_dle->errstr();

    print "\nDLE:\n";
    my $all_row_dle = $sth_dle->fetchall_arrayref();
    for my $row_dle (@{$all_row_dle}) {
	print " $row_dle->[0] $row_dle->[1] $row_dle->[2] $row_dle->[3]\n";
    }

    my $sth_image = $dbh->prepare("SELECT * FROM image ORDER BY dump_timestamp, level")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_image->execute() or die "Cannot execute: " . sth_image->errstr();

    print "\nIMAGE:\n";
    my $all_row_image = $sth_image->fetchall_arrayref();
    for my $row_image (@{$all_row_image}) {
	print " $row_image->[0] $row_image->[1] $row_image->[2] $row_image->[3] $row_image->[4] $row_image->[5]\n";
    }

    my $sth_volume = $dbh->prepare("SELECT * FROM volume")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_volume->execute() or die "Cannot execute: " . sth_volume->errstr();

    print "\nVOLUME:\n";
    my $all_row_volume = $sth_volume->fetchall_arrayref();
    for my $row_volume (@{$all_row_volume}) {
	print " $row_volume->[0] $row_volume->[1] $row_volume->[2] $row_volume->[3]\n";
    }

    my $sth_copy = $dbh->prepare("SELECT * FROM copy")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_copy->execute() or die "Cannot execute: " . sth_copy->errstr();

    print "\nCOPY:\n";
    my $all_row_copy = $sth_copy->fetchall_arrayref();
    for my $row_copy (@{$all_row_copy}) {
	print " $row_copy->[0] $row_copy->[1] $row_copy->[2] $row_copy->[3] $row_copy->[4] $row_copy->[5] $row_copy->[6]\n";
    }

    my $sth_part = $dbh->prepare("SELECT * FROM part")
	or die "Cannot prepare: " . $dbh->errstr();
    $sth_part->execute() or die "Cannot execute: " . sth_part->errstr();

    print "\nPART:\n";
    my $all_row_part = $sth_part->fetchall_arrayref();
    for my $row_part (@{$all_row_part}) {
	print " $row_part->[0] $row_part->[1] $row_part->[2] $row_part->[3] $row_part->[4] $row_part->[5] $row_part->[6] $row_part->[7]\n";
    }
}


sub _validate {
    my $catalog = shift;

    $catalog->validate();
}

sub _retention {
    my $catalog = shift;

    $catalog->compute_retention();
}

sub _export {
    my $catalog = shift;
    my $filename = shift;

    if (!($filename =~ /^\//)) {
	$filename = Amanda::Util::get_original_cwd() . "/" . $filename;
    }
    $catalog->export_to_file($filename);
}

sub _import {
    my $catalog = shift;
    my $filename = shift;

    if (!($filename =~ /^\//)) {
	$filename = Amanda::Util::get_original_cwd() . "/" . $filename;
    }
    $catalog->import_from_file($filename);
}

sub _merge {
    my $catalog = shift;

    $catalog->merge();
}

sub _clean {
    my $catalog = shift;

    # similar to Amanda::Rest::Runs::list
    my $Amanda_process = Amanda::Process->new();
    $Amanda_process->load_ps_table();

    my $logdir = config_dir_relative(getconf($CNF_LOGDIR));
    my @tracefiles = (<$logdir/amdump.*>, <$logdir/fetchdump.*>, <$logdir/checkdump.*> );
    foreach my $tracefile (@tracefiles) {
        my $timestamp;
        if ($tracefile =~ /amdump\.(.*)$/) {
            $timestamp = $1;
        }
        if (!defined $timestamp) {
            if ($tracefile =~ /checkdump\.(.*)$/) {
                $timestamp = $1;
            }
        }
        if (!defined $timestamp) {
            if ($tracefile =~ /fetchdump\.(.*)$/) {
                $timestamp = $1;
            }
        }
        next if length($timestamp) != 14;
        my $logfile = "$logdir/log.$timestamp.0";

	# similar to Amanda::Rest::Runs::list_one
	next if !-f $logfile;
	my $run_type;
        my $pname;
        my $pid;
        my $status = "running";
        my $message_file;
	open (LOG, "<", $logfile);
	while (my $line = <LOG>) {
            if ($line =~ /^START planner date (\d*)$/) {
                if (!defined $timestamp) {
                    $timestamp = $1;
                }
                if (!defined $tracefile) {
                    $tracefile = "$logdir/amdump.$timestamp";
                }
            } elsif ($line =~ /^INFO (.*) amdump pid (\d*)$/) {
                $run_type = "amdump";
                $pname = $1;
                $pid = $2;
            } elsif ($line =~ /^INFO (.*) fork amdump (\d*) (\d*)$/) {
                $run_type = "amdump";
                $pname = $1;
                $pid = $3;
            } elsif ($line =~ /^INFO (.*) amflush pid (\d*)$/) {
                $run_type = "amflush";
                $pname = $1;
                $pid = $2;
            } elsif ($line =~ /^INFO (.*) fork amflush (\d*) (\d*)$/) {
                $run_type = "amflush";
                $pname = $1;
                $pid = $3;
            } elsif ($line =~ /^INFO (.*) amvault pid (\d*)$/) {
                $run_type = "amvault";
                $pname = $1;
                $pid = $2;
            } elsif ($line =~ /^INFO (.*) fork amvault (\d*) (\d*)$/) {
                $run_type = "amvault";
                $pname = $1;
                $pid = $3;
            } elsif ($line =~ /^INFO (.*) checkdump pid (\d*)$/) {
                $run_type = "checkdump";
                $pname = $1;
                $pid = $2;
            } elsif ($line =~ /^INFO (.*) fork checkdump (\d*) (\d*)$/) {
                $run_type = "checkdump";
                $pname = $1;
                $pid = $3;
            } elsif ($line =~ /^INFO (.*) fetchdump pid (\d*)$/) {
                $run_type = "fetchdump";
                $pname = $1;
                $pid = $2;
            } elsif ($line =~ /^INFO (.*) fork fetchdump (\d*) (\d*)$/) {
                $run_type = "fetchdump";
                $pname = $1;
                $pid = $3;
            } elsif ($line =~ /^INFO .* message_file (.*)$/) {
                $message_file = $1;
            } elsif ($line =~ /^INFO .* fork .* (\d*) (\d*)$/) {
                $pid = $2 if $pid == $1;
            }
            if (defined $run_type and
                $line =~ /^INFO $pname pid-done $pid/) {
                $status = "done";
            }
        }
	close(LOG);
        next if !$run_type;

        if ($status eq "running" and $pid) {
            $status = "aborted" if !$Amanda_process->process_alive($pid, $pname);
        }

	if ($status eq "running") {
	    my $msg = "An Amanda process is already running - 'clean' command can't be run while amanda is running.";
	    debug($msg);
	    print $msg . "\n";
	    return;
	}
    }

    $catalog->clean();
}

sub _remove_working_cmd {
    my $catalog = shift;
    my $pid = shift;

    $catalog->remove_working_cmd($pid);
}

sub _remove_cmd {
    my $catalog = shift;
    my $id = shift;

    $catalog->remove_cmd($id);
}

sub _add_flush_cmd {
    my $catalog = shift;
    my $config = shift;
    my $holding_file = shift;
    my $hostname = shift;
    my $diskname = shift;
    my $dump_timestamp = shift;
    my $level = shift;
    my $dst_storage = shift;
    my $working_pid = shift;
    my $status = shift;

    my $id = $catalog->add_flush_cmd(
		config => $config,
		holding_file => $holding_file,
		hostname => $hostname,
		diskname => $diskname,
		dump_timestamp => $dump_timestamp,
		level => $level,
		dst_storage => $dst_storage,
		working_pid => $working_pid,
		status => $status);
    print "$id\n";
}

sub _add_copy_cmd {
    my $catalog = shift;
    my $config = shift;
    my $src_storage = shift;
    my $label = shift;
    my $hostname = shift;
    my $diskname = shift;
    my $dump_timestamp = shift;
    my $level = shift;
    my $dst_storage = shift;
    my $working_pid = shift;
    my $status = shift;
    my $size = shift;
    my $start_time = shift;

    my $id = $catalog->add_copy_cmd(
		config => $config,
		src_storage => $src_storage,
		label => $label,
		hostname => $hostname,
		diskname => $diskname,
		dump_timestamp => $dump_timestamp,
		level => $level,
		dst_storage => $dst_storage,
		working_pid => $working_pid,
		status => $status,
		size => $size,
		start_time => $start_time);
    print "$id\n";
}

sub __write_command {
    my $cmd = shift;

    my $status;
    if ($cmd->{'status'} == $Amanda::Cmdfile::CMD_DONE) {
	$status = "DONE";
    } elsif ($cmd->{'status'} == $Amanda::Cmdfile::CMD_TODO) {
	$status = "TODO";
    } elsif ($cmd->{'status'} == $Amanda::Cmdfile::CMD_PARTIAL) {
	$status = "PARTIAL:$cmd->{'size'}";
    } else {
	$status = "";
    }
    my $config = quote_string($cmd->{'config_name'});
    my $hostname = quote_string($cmd->{'hostname'});
    my $diskname = quote_string($cmd->{'diskname'});
    my $dump_timestamp = quote_string(''.$cmd->{'dump_timestamp'});
    my $dst_storage = quote_string($cmd->{'dst_storage'});
    if ($cmd->{'operation'} == $Amanda::Cmdfile::CMD_FLUSH) {
	my $holding_file = quote_string($cmd->{'holding_file'});
	print "$cmd->{'id'} FLUSH $config $holding_file $hostname $diskname $dump_timestamp $cmd->{'level'} $dst_storage WORKING:$cmd->{'working_pid'} $status\n";
    } elsif ($cmd->{'operation'} == $Amanda::Cmdfile::CMD_COPY) {
	my $src_storage = quote_string($cmd->{'src_storage'});
	my $src_pool = quote_string($cmd->{'src_pool'});
	my $src_label = quote_string($cmd->{'src_label'});
	my $src_labels_str = quote_string($cmd->{'src_labels_str'});
	print "$cmd->{'id'} COPY $config $src_storage $src_pool $src_label $cmd->{'src_fileno'} $src_labels_str $cmd->{'start_time'} $hostname $diskname $dump_timestamp $cmd->{'level'} $dst_storage WORKING:$cmd->{'working_pid'} $status\n";
    } elsif ($cmd->{'operation'} == $Amanda::Cmdfile::CMD_RESTORE) {
	my $src_storage = quote_string($cmd->{'src_storage'});
	my $src_pool = quote_string($cmd->{'src_pool'});
	if ($cmd->{'src_pool'} eq "HOLDING") {
	    my $holding_file = quote_string($cmd->{'holding_file'});
	    print "$cmd->{'id'} RESTORE $config $src_storage $src_pool $holding_file 0 $cmd->{'expire'} $hostname $diskname $dump_timestamp $cmd->{'level'} $dst_storage WORKING:$cmd->{'working_pid'} $status\n";
	} else {
	    my $label = quote_string($cmd->{'label'});
	    print "$cmd->{'id'} RESTORE $config $src_storage $src_pool $label $cmd->{'src_fileno'} $cmd->{'expire'} $hostname $diskname $dump_timestamp $cmd->{'level'} $dst_storage WORKING:$cmd->{'working_pid'} $status\n";
	}
    }
}

sub _get_command_from_id {
    my $catalog = shift;
    my $id = shift;

    my $cmd = $catalog->get_command_from_id($id);
    __write_command($cmd);
}

sub _get_flush_command {
    my $catalog = shift;
    my $id = shift;

    my $cmds = $catalog->get_flush_command();
    foreach my $cmd (@$cmds) {
	__write_command($cmd);
    }
}

sub _get_copy_command {
    my $catalog = shift;
    my $id = shift;

    my $cmds = $catalog->get_copy_command();
    foreach my $cmd (@$cmds) {
	__write_command($cmd);
    }
}

sub _get_command_ids_for_holding {
    my $catalog = shift;
    my $hfile = shift;

    my $ids = $catalog->get_command_ids_for_holding($hfile);
    print join(' ',@$ids),"\n" if @$ids;
}

sub _add_image {
    my $catalog = shift;
    my $hostname = shift;
    my $diskname = shift;
    my $device = shift;
    my $dump_timestamp = shift;
    my $level = shift;
    my $based_on_timestamp = shift;
    my $pid = shift;

    my $image = $catalog->add_image($hostname, $diskname, $device,
				    $dump_timestamp, $level,
				    $based_on_timestamp, $pid);
    debug("_add_image: $image->{'image_id'}");
    print "$image->{'image_id'} $image->{'disk_id'}\n";
}

sub _finish_image {
    my $catalog        = shift;
    my $image_id       = shift;
    my $disk_id        = shift;
    my $orig_kb        = shift;
    my $dump_status    = shift;
    my $nb_files       = shift;
    my $nb_directories = shift;
    my $native_crc     = shift;
    my $client_crc     = shift;
    my $server_crc     = shift;
    my $message        = shift;

    debug("_finish_image $image_id");
    my $image = $catalog->get_image($image_id, $disk_id);
    $image->finish_image($orig_kb, $dump_status, $nb_files, $nb_directories,
			 $native_crc, $client_crc, $server_crc, $message);
    print "$image_id\n";
}

sub _add_copy {
    my $catalog = shift;
    my $image_id = shift;
    my $write_timestamp = shift;
    my $copy_pid = shift;

    my $image = $catalog->get_image($image_id);
    my $copy = $image->add_copy($write_timestamp, $copy_pid);
    print "$copy->{'copy_id'}\n";
}

sub _finish_copy {
    my $catalog = shift;
    my $copy_id = shift;
    my $nparts = shift;
    my $kb = shift;
    my $bytes = shift;
    my $copy_status = shift;

    my $copy = $catalog->get_copy($copy_id);
    $copy->finish_copy($nparts, $kb, $bytes, $copy_status);
    print "$copy_id\n";
}

sub _add_volume {
    my $catalog = shift;
    my $label = shift;
    my $write_timestamp = shift;

    my $volume = $catalog->add_volume($label, $write_timestamp);
    print "$volume->{'volume_id'}\n";
}

sub _add_part {
    my $catalog = shift;
    my $volume_id = shift;
    my $copy_id = shift;
    my $part_offset = shift;
    my $part_size = shift;
    my $filenum = shift;
    my $part_num = shift;
    my $part_status = shift;

    my $volume = $catalog->get_volume($volume_id);
    my $copy = $catalog->get_copy($copy_id);
    $copy->add_part($volume, $part_offset, $part_size, $filenum, $part_num, $part_status);
}

sub _rm_volume {
    my $catalog = shift;
    my $pool = shift;
    my $label = shift;

    $catalog->remove_volume($pool, $label);
}

sub _dump {
    my $catalog = shift;
    my $argv = shift;
    shift @$argv;

    my @dumpspecs = Amanda::Cmdline::parse_dumpspecs([@$argv],
			$Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP);
    print "config dump_timestamp hostname diskname level storage dump_status copy_status nb_files nb_directories\n";
    $catalog->print_catalog(\@dumpspecs, dump => 1, all_configs => $opt_all_configs, exact_match => $opt_exact_match, timestamp => $opt_timestamp);
}

sub _part {
    my $catalog = shift;
    my $argv = shift;
    shift @$argv;

    my @dumpspecs = Amanda::Cmdline::parse_dumpspecs([@$argv],
			$Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP);
    print "config dump_timestamp hostname diskname level storage pool label dump_status copy_status part_status filenum nb_parts partnum nb_files nb_directories\n";
    $catalog->print_catalog(\@dumpspecs, parts => 1, all_configs => $opt_all_configs, exact_match => $opt_exact_match, timestamp => $opt_timestamp);
}

sub _get_log_names {
    my $catalog = shift;

    my $volumes = $catalog->find_volumes(
			config => get_config_name());

    my %log_names;
    for my $volume (@$volumes) {
	my $log_name = "log.$volume->{'write_timestamp'}.0";
	$log_names{$log_name} += 1;
    }

    for my $log_name (keys %log_names) {
	print "$log_name\n";
    }
}

sub _write_tapelist {
    my $catalog = shift;

    $catalog->write_tapelist(1);
}

sub _rm_config {
    my $catalog = shift;
    my $config_name = shift;
    $config_name = get_config_name() if !defined $config_name;

    if ($opt_remove_pool) {
	foreach my $storage_name (getconf_list("storage")) {
#print "storage_name $storage_name\n";
	    my $st = lookup_storage($storage_name);
	    if ($st) {
		my $pool_name = storage_getconf($st, $STORAGE_TAPEPOOL);
#print "pool_name: $pool_name\n";
		if ($pool_name) {
		    $catalog->rm_pool(pool => $pool_name);
		}
	    }
	}
    }
    $catalog->rm_config(config_name => $config_name);
}

sub _rm_pool {
    my $catalog = shift;
    my $pool_name = shift;

    $catalog->rm_pool(pool => $pool_name);
}

my $catalog;
my $catalog_conf;
my $catalog_name = getconf($CNF_CATALOG);
debug("catalog_name: $catalog_name");

select(STDOUT); # default
$| = 1;

if ($opt_interactive) {
    while (my $line=<STDIN>) {
	debug("line: $line");
	chomp $line;
	my @argv = split_quoted_strings($line);
	my $old_timestamp = $opt_timestamp;
	my $old_exact_match = $opt_exact_match;
	my $old_all_configs = $opt_all_configs;
	if (grep { $_ eq '--timestamp' } @argv) {
	    $opt_timestamp = 1;
	    @argv = grep { $_ ne '--timestamp' } @argv;
	}
	if (grep { $_ eq '--exact-match' } @argv) {
	    $opt_exact_match = 1;
	    @argv = grep { $_ ne '--exact-match' } @argv;
	}
	if (grep { $_ eq '--all-configs' } @argv) {
	    $opt_all_configs = 1;
	    @argv = grep { $_ ne '--all-configs' } @argv;
	}
	print "BEGIN\n";
	run_command(@argv);
	print "\nEND\n";
	$opt_timestamp = $old_timestamp;
	$opt_exact_match = $old_exact_match;
	$opt_all_configs = $old_all_configs;
    }
} else {
    my @argv = @ARGV[1 .. $#ARGV];
    run_command(@argv);
}

sub run_command {
    my @argv = @_;
    my $command = $argv[0];

    debug("command: " . join(' ', @argv));

    $catalog_conf = lookup_catalog($catalog_name) if $catalog_name && !$catalog_conf;
    if ($command eq "create") {
	my $drop_tables = 0;
	$drop_tables = 1 if $argv[1] eq 'drop_tables';
	$catalog = Amanda::DB::Catalog2->new($catalog_conf, config_name => $opt_config,
							    drop_tables => $drop_tables,
							    create => 1,
							    load => 1);
	return;
    } elsif ($command eq "import") {
	$catalog = Amanda::DB::Catalog2->new($catalog_conf, config_name => $opt_config,
							    create => 1,
							    empty => 1);
	_import($catalog, $argv[1]);
	return;
    } elsif ($command eq "upgrade") {
	$catalog = Amanda::DB::Catalog2->new($catalog_conf, config_name => $opt_config,
							    upgrade => 1);
	return;
    }
    $catalog = Amanda::DB::Catalog2->new($catalog_conf, config_name => $opt_config) if !$catalog;
    my $dbh = $catalog->{'dbh'};

    if ($command eq "validate") {
	_validate($catalog);
    } elsif ($command eq "export") {
	_export($catalog, $argv[1]);
    } elsif ($command eq "merge") {
	_merge($catalog);
    } elsif ($command eq "retention") {
	_retention($catalog);
    } elsif ($command eq "clean") {
	_clean($catalog);
    } elsif ($command eq "add-image") {
	_add_image($catalog, $argv[1], $argv[2], $argv[3], $argv[4], $argv[5], $argv[6], $argv[7]);
    } elsif ($command eq "finish-image") {
	_finish_image($catalog, $argv[1], $argv[2], $argv[3], $argv[4], $argv[5], $argv[6], $argv[7], $argv[8], $argv[9]);
    } elsif ($command eq "add-copy") {
	_add_copy($catalog, $argv[1], $argv[2], $argv[3]);
    } elsif ($command eq "finish-copy") {
	_finish_copy($catalog, $argv[1], $argv[2], $argv[3], $argv[4], $argv[5]);
    } elsif ($command eq "add-volume") {
	_add_volume($catalog, $argv[1], $argv[2]);
    } elsif ($command eq "add-part") {
	_add_part($catalog, $argv[1], $argv[2], $argv[3], $argv[4], $argv[5], $argv[6], $argv[7]);
    } elsif ($command eq "rm-volume") {
	_rm_volume($catalog, $argv[1], $argv[2]);
    } elsif ($command eq "dump") {
	_dump($catalog, \@argv);
    } elsif ($command eq "part") {
	_part($catalog, \@argv);
    } elsif ($command eq "out") {
	_out($catalog);
    } elsif ($command eq "remove-working-cmd") {
	_remove_working_cmd($catalog, $argv[1]);
    } elsif ($command eq "remove-cmd") {
	_remove_cmd($catalog, $argv[1]);
    } elsif ($command eq "add-flush-cmd") {
	_add_flush_cmd($catalog, $argv[1], $argv[2], $argv[3], $argv[4], $argv[5], $argv[6], $argv[7], $argv[8], $argv[9]);
    } elsif ($command eq "add-copy-cmd") {
	_add_copy_cmd($catalog, $argv[1], $argv[2], $argv[3], $argv[4], $argv[5], $argv[6], $argv[7], $argv[8], $argv[9], $argv[10], $argv[11], $argv[12]);
    #} elsif ($command eq "add-restore-cmd") {
	#_add_restore_cmd($catalog, $argv[1], $argv[2], $argv[3], $argv[4], $argv[5], $argv[6], $argv[7], $argv[8], $argv[9]);
    } elsif ($command eq "get-cmd-from-id") {
	_get_command_from_id($catalog, $argv[1]);
    } elsif ($command eq "get-flush-cmd") {
	_get_flush_command($catalog);
    } elsif ($command eq "get-copy-cmd") {
	_get_copy_command($catalog);
    } elsif ($command eq "get-cmd-ids-for-holding") {
	_get_command_ids_for_holding($catalog, $argv[1]);
    } elsif ($command eq "get-log-names") {
	_get_log_names($catalog);
    } elsif ($command eq "write-tapelist") {
	_write_tapelist($catalog);
    } elsif ($command eq "rm-config") {
	_rm_config($catalog, $argv[1]);
    } elsif ($command eq "rm-pool") {
	_rm_pool($catalog, $argv[1]);
    } else {
	print "Bad '$command' command\n";
	debug("Bad '$command' command");
	usage();
    }
}

$catalog->quit() if defined $catalog;

Amanda::Util::finish_application();
exit($exit_status);
