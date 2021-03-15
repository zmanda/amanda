#!@PERL@
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

use lib '@amperldir@';
use strict;
use warnings;
use Getopt::Long;

package Amanda::Application::Amsamba;
use base qw(Amanda::Application);
use File::Copy;
use File::Path;
use IPC::Open2;
use IPC::Open3;
use Sys::Hostname;
use Symbol;
use IO::Handle;
use MIME::Base64 ();
use Amanda::Constants;
use Amanda::Config qw( :init :getconf  config_dir_relative );
use Amanda::Debug qw( :logging );
use Amanda::Paths;
use Amanda::Util qw( :constants :quoting);
use Amanda::MainLoop qw( :GIOCondition );

# skip first cmd, and snip out anything after \n if present in an arg
our (@CLI_ARGV) = map {
       s{[\n].*}{};
       ( $_ ne "" ? ($_) : ());  # snip out empty strings.. (?)
    } ( @ARGV[1..$#ARGV] );

# must not evaluate undef-uncreated vars using ne or eq
# BUT ... must also differentiate between defined-but-"" and undef itself..
sub isNull($) {
    return !( $_[0] // "" ); # NOT false if not defined AND if not null-string
}
sub notNull($) {
    return $_[0] // "";  # false if not defined OR if null-string
}

sub new {
    my $class = shift;
    my ($config, $host, $disk, $device, $level, $index, $message, $collection, $record, $calcsize, $gnutar_path, $smbclient_path, $amandapass, $exclude_file, $exclude_list, $exclude_optional, $include_file, $include_list, $include_optional, $recover_mode, $allow_anonymous, $target, $regex_match) = @_;
    my $self = $class->SUPER::new($config);

    $self->{gnutar}     = $Amanda::Constants::GNUTAR;
    $self->{smbclient}  = $Amanda::Constants::SAMBA_CLIENT;
    $self->{amandapass} = "$Amanda::Paths::CONFIG_DIR/amandapass";

    $self->{config}       = $config;
    $self->{host}         = $host;
    $self->{disk}         = $device;
    $self->{device}       = $disk;

    $self->{level}            = [ @{$level} ];
    $self->{index}            = $index;
    $self->{message}          = $message;
    $self->{collection}       = $collection;
    $self->{record}           = $record;
    $self->{calcsize}         = $calcsize;
    $self->{exclude}          = [];
    $self->{exclude_file}     = [ @{$exclude_file} ];
    $self->{exclude_list}     = [ @{$exclude_list} ];
    $self->{exclude_optional} = $exclude_optional;
    $self->{include}          = [];
    $self->{include_file}     = [ @{$include_file} ];
    $self->{include_list}     = [ @{$include_list} ];
    $self->{include_optional} = $include_optional;
    $self->{recover_mode}     = $recover_mode;
    $self->{allow_anonymous}  = $allow_anonymous;
    $self->{target}           = $target;
    $self->{regex_match}    = 0;

    $self->{gnutar}     = $gnutar_path
        if (notNull($gnutar_path));
    $self->{smbclient}  = $smbclient_path
        if (notNull($smbclient_path));
    $self->{amandapass}  = config_dir_relative($amandapass)
        if (notNull($amandapass));
    $self->{disk}         = $disk
        if (notNull($disk));
    $self->{device}       = $device
        if (notNull($device));
    $self->{regex_match} = 1
        if ($regex_match && $regex_match =~ /^yes$/i);

    return $self;
}

# on entry:
#   $self->{exclude_file}
#   $self->{exclude_list}
#   $self->{include_file}
#   $self->{include_list}
#on exit:
#  $self->{exclude}
#  $self->{include}
#  $self->{include_filename}
sub validate_inexclude {
    my $self = shift;
    my ($n_incls) = @{$self->{include_file}} + @{$self->{include_list}};
    my ($n_excls) = @{$self->{exclude_file}} + @{$self->{exclude_list}};

    if ($n_incls > 0 && $n_excls > 0) {
	$self->print_to_server_and_die("Can't have both include and exclude",
				       $Amanda::Script_App::ERROR);
    }

    my $set = ( $n_incls > 0 ? $self->{include} : $self->{exclude} );
    my $setflists = ( $n_incls > 0 ? $self->{include_list} : $self->{exclude_list} );
    my $setitems = ( $n_incls > 0 ? $self->{include_file} : $self->{exclude_file} );
    my $setoptional = ( $n_incls > 0 ? $self->{include_optional} : $self->{exclude_optional} );

    # add the onesy-twosy set ... if they exist
    push @{$set}, @{$setitems};

    my ($linesep) = $/;

    if ($self->{action} eq 'check' && !$setoptional) {
        # message about failed files...
        foreach my $file (@{$setflists}) {
            open(FF, $file) && close(FF) && next;
            $self->print_to_server("Open of '$file' failed: $!",
                                   $Amanda::Script_App::ERROR);
        }
    }

    # slurp the contents of files
    foreach my $file (@{$setflists}) {
        local $/ = undef;
        open(FF, $file) || next;
        push @{$set}, split($linesep,<FF>);
	close(FF);
    }

    #
    # include set (*only*) is appended for restore action...
    #
    push @{$self->{include}}, map { s{\\([0-7]{3})}{chr oct $1}eg; } @::CLI_ARGV
        if ($self->{action} eq "restore");

    #
    # use subdir as prefix for everything with a "./" or "." (alone) at start
    #
    push @{$set}, map { s{^\.(/|\z)}{$self->{subdir}$1}; } @{$set}
       if ( notNull($self->{subdir}) );

    # need to get the total length if > 512 chars (i.e. a long cmd line)
    my ($len) = 0;
    for my $i ( @{$set} ) {
       $len += length($_);
       last if ( $len > 512 );
    }

    # don't need a single include-file??
    return if ($n_excls);
    return if ($self->{action} ne "restore");
    return if ($len <= 512);

    # we do need one...

    # put all include in a single file $self->{include_filename}
    $self->{include_filename} = "@amdatadir@/tmp/amsamba.$$.include";
    open FULL_FILE, ">$self->{include_filename}";
    print FULL_FILE join($linesep,@{$set});
    # add command line include for amrestore
    close FULL_FILE;
}

sub normalize_share {
    my $to_parse = $_[0];
    my (@flds) = grep( m{.}, split(m{[\\/]+},$to_parse) );
    my ($host,$share,$stdshare,$subdir);

    return
    	if (! @flds);

    $subdir = $to_parse;
    $subdir =~ s{^([\\/]+[^\\/]+)}{}; # slice off first field 
    $subdir =~ s{^([\\/]+[^\\/]+)}{}; # slice off second field (if any)

    if ($to_parse =~ m{^/{2}}) {
	$share      = $host = "//$flds[0]";
	$share      .= "/$flds[1]" if ( $flds[1] // "" );
	$stdshare   = $share;
	$stdshare   = tr{/}{\\};
    } else {
	$share      = $host = "\\\\$flds[0]";
	$share      .= "\\$flds[1]" if ( $flds[1] // "" );
	$stdshare   = $share;
    }

    return { 'host' => $host, 'share' => $share, 'stdshare' => $stdshare, 'subdir' => $subdir };
}

# on entry:
#   $self->{target} == //host/share/subdir           \\host\share\subdir
#   or
#   $self->{device}    == //host/share/subdir		\\host\share\subdir
# on exit:
#   $self->{cifshost}   = //host			\\host
#   $self->{share}      = //host/share			\\host\share
#   $self->{sambashare} = \\host\share			\\host\share
#   $self->{subdir}     = subdir			subdir
sub parsesharename {
    my $self = shift;

    my $to_parse = ($self->{target} // "") || ($self->{device} // "");

    my ($norm) = normalize_share($to_parse);

    return
    	if (isNull($norm));

    $self->{unc}         = ( $norm->{"host"} =~ m{^/{2}} ) ? 0 : 1;
    $self->{cifshost}    = $norm->{host};
    $self->{share}       = $norm->{share};
    $self->{sambashare}  = $norm->{stdshare};
    $self->{subdir}      = $norm->{subdir};
}


# Read $self->{amandapass} file.
# on entry:
#   $self->{cifshost} == //host/share
#   $self->{share} == //host/share
# on exit:
#   $self->{domain}   = domain to connect to.
#   $self->{username} = username (-U)
#   $self->{password} = password
sub findpass {
    my $self = shift;

    my $amandapass;
    my $line;

    $self->{domain} = undef;
    $self->{username} = undef;
    $self->{password} = undef;

    debug("amandapass: $self->{amandapass}");
    if (!open($amandapass, $self->{amandapass})) {
	if ($self->{allow_anonymous}) {
            $self->{username} = $self->{allow_anonymous};
            debug("cannot open password file '$self->{amandapass}': $!\n");
            debug("Using anonymous user: $self->{username}");
            return;
        }
        $self->print_to_server_and_die(
                    "cannot open password file '$self->{amandapass}': $!",
                    $Amanda::Script_App::ERROR);
        return;

    }

    my ($diskname, $userpasswd, $domain, $extra);

    while ($line = <$amandapass>) {
	chomp $line;
	next if $line =~ /^#/;

	($diskname, $userpasswd, $domain, $extra) = Amanda::Util::split_quoted_string_friendly($line);

        debug("Trailling characters ignored in amandapass line: $extra")
            if ($extra);

        my $diskpath = normalize_share($diskname);

        next if (isNull($diskpath));

        last if ($diskname eq '*');
        last if ($diskpath->{stdshare} eq $self->{sambashare});
        last if ($diskpath->{stdshare} =~ m{\\[*]$} && $diskpath->{host} eq $self->{cifshost});
    }

    close($amandapass);

    if (!$line && !$self->{allow_anonymous}) {
        $self->print_to_server_and_die(
            "Cannot find password for share $self->{share} in $self->{amandapass}",
            $Amanda::Script_App::ERROR);
        return;                                             ##### RETURN #####
    }

    if (!$line) {
        $self->{username} = $self->{allow_anonymous};
        debug("Cannot find password for share $self->{share} in $self->{amandapass}");
        debug("Using anonymous user: $self->{username}");
        return;                                             ##### RETURN #####
    }

    #
    # found an entry...
    #

    if (isNull($userpasswd)) {
        $self->{username} = "guest";
        return;                                          ##### RETURN #####
    }

    $self->{domain} = $domain
       if (notNull($domain));

    my ($username, $password) = split('%', $userpasswd, 2);

    $username =~ s{@*$}{};

    $self->{username} = $username;
    if ($password =~ /^6G\!dr(.*)/) {
        my $base64 = $1;
        $password = MIME::Base64::decode($base64);
        chomp($password);
    }
    $self->{password} = $password
       if (notNull($password));

    ( $self->{username} =~ s{\@(\w.+)$}{} ) && ( $self->{domain} = $1)
	if ( isNull($self->{domain}) );

}

sub command_support {
    my $self = shift;

    print "CONFIG YES\n";
    print "HOST YES\n";
    print "DISK YES\n";
    print "MAX-LEVEL 1\n";
    print "INDEX-LINE YES\n";
    print "INDEX-XML NO\n";
    print "MESSAGE-LINE YES\n";
    print "MESSAGE-XML NO\n";
    print "RECORD YES\n";
    print "COLLECTION NO\n";
    print "MULTI-ESTIMATE NO\n";
    print "CALCSIZE NO\n";
    print "CLIENT-ESTIMATE YES\n";
    print "EXCLUDE-FILE YES\n";
    print "EXCLUDE-LIST YES\n";
    print "EXCLUDE-OPTIONAL YES\n";
    print "INCLUDE-FILE YES\n";
    print "INCLUDE-LIST YES\n";
    print "INCLUDE-OPTIONAL YES\n";
    print "RECOVER-MODE SMB\n";
}

sub spawn_smbclient {
    my ($self, $ref_stdout,$ref_stderr,@call_args) = @_;
    # my ($password_rdr, $password_wtr);
    my ($authstring);
    my ($sys_lc_ctype) = ( exists($ENV{LC_CTYPE}) ? $ENV{LC_CTYPE} : undef );
    my ($err);
    my $pid;

    my @cmd = ($self->{smbclient});

    # create no-close-on-exec flags
    # ... add 8 more FDs (to have room)
    #$^F = 10;
    # pipe($password_rdr, $password_wtr);
    #$^F = 2;
    # $^F -= 8;  # put back again

    my $tmpfile = qx{mktemp -p @amdatadir@/tmp};
    chomp $tmpfile;
    chmod(0600,$tmpfile);

    $self->{tmpfile} = $tmpfile;

    # prepare command ....
    push @cmd, $self->{share};

    # share-specific password is defined-as-empty if no password sent..
    push @cmd, "" if (isNull($self->{password})); #
    # push @cmd, "-m", "SMB2";
    push @cmd, "-b", "0";
    push @cmd, "-N";
    push @cmd, "-A", $tmpfile;
    push @cmd, "-E";                        # stderr output
    push @cmd, "-D", $self->{subdir}
        if (notNull($self->{subdir}));

    push @cmd, @call_args; # and all the rest...

    debug("to execute: '" . $self->{smbclient} . "' '" .
          join("' '", @cmd)."'");

    # no reason to fear buffering for this little file (UNIX format!)
    $authstring .= "username=$self->{username}\n";
    $authstring .= "password=$self->{password}\n"
       if ( notNull($self->{password}) );
    $authstring .= "domain=" . uc($self->{domain}). "\n"
        if (notNull($self->{domain}));

    debug("No password")
        if (isNull($self->{password}));

    # write and close w/reader open(!)
    # prevents SIGPIPE because reader is still open
    open(AUTH,">$tmpfile");
    print AUTH ($authstring);
    close AUTH;

    my ($stdin) = ( grep { m{^-\w*x} } @cmd ) ? "<&STDIN" : "<&DEVNULL";

    eval {
        open(DEVNULL,"+</dev/null");

        # simpler to pass the fd...
        my ($devnull) = ">&DEVNULL";
        $ref_stdout = \$devnull if ( not ref $ref_stdout ); # ref-to-string .. becomes glob
        $ref_stderr = \$devnull if ( not ref $ref_stderr ); # ref-to-string .. becomes glob

        # handle un-defined state like ||
	$$ref_stdout //= Symbol::gensym;
	$$ref_stderr //= Symbol::gensym;

	# debug(sprintf("smbclient cmd: ($stdin / $$ref_stdout / $$ref_stderr) auth file string\n$authstring"));

        $ENV{LC_CTYPE} = 'en_US.UTF-8';
        $pid = open3($stdin, $$ref_stdout, $$ref_stderr, @cmd);
        delete $ENV{LC_CTYPE};

        debug(sprintf("smbclient cmd: done pid=%d %s %s %s: $!",$pid, $stdin, $$ref_stdout,$$ref_stderr));

        close(DEVNULL);
        $ENV{LC_CTYPE} = $sys_lc_ctype
            if ( defined($sys_lc_ctype) );

        1;  # dont fail this block
    } || do {
        close(DEVNULL);
        delete $ENV{LC_CTYPE};
        $ENV{LC_CTYPE} = $sys_lc_ctype
            if ( defined($sys_lc_ctype) );
        debug(sprintf("ERROR: smbclient cmd: r=$pid '%s' stdin=%s stdout=%s stderr=%s",join("' '",@cmd),$stdin, $$ref_stdout,$$ref_stderr));
        return undef;
    };

    debug(sprintf("INFO: smbclient ok cmd: r=$pid '%s' stdin=%s stdout=%s stderr=%s",join("' '",@cmd),$stdin, $$ref_stdout,$$ref_stderr));

    # now process has its own FD for reader...
    # $password_rdr->close() if ( $password_rdr );
    # caller must *wait* on the process..
    return $pid;
}


sub command_selfcheck {
    my $self = shift;

    $self->print_to_server("disk " . quote_string($self->{disk}),
			   $Amanda::Script_App::GOOD);

    $self->print_to_server("amsamba version " . $Amanda::Constants::VERSION,
			   $Amanda::Script_App::GOOD);
    #check binary
    if (isNull($self->{smbclient})) {
	$self->print_to_server(
	    "smbclient not set; you must define the SMBCLIENT-PATH property",
	    $Amanda::Script_App::ERROR);
    }
    elsif (! -e $self->{smbclient}) {
	$self->print_to_server("$self->{smbclient} doesn't exist",
			       $Amanda::Script_App::ERROR);
    }
    elsif (! -x $self->{smbclient}) {
	$self->print_to_server("$self->{smbclient} is not executable",
			       $Amanda::Script_App::ERROR);
    } else {
	my @sv = `$self->{smbclient} --version`;
	if ($? >> 8 == 0) {
	    $sv[0] =~ /^[^0-9]*(.*)$/;
	    my $sv = $1;
	    $self->print_to_server("amsamba smbclient-version $sv",
				   $Amanda::Script_App::GOOD);
	} else {
	    $self->print_to_server(
		"[Can't get " . $self->{smbclient} . " version]\n",
		$Amanda::Script_App::ERROR);
	}
    }

    $self->print_to_server("$self->{smbclient}",
			   $Amanda::Script_App::GOOD);

    return if (isNull($self->{disk}));
    return if (isNull($self->{device}));

    $self->parsesharename();
    $self->findpass();
    $self->validate_inexclude();

    print "OK " . $self->{share} . "\n";
    print "OK " . $self->{device} . "\n";
    print "OK " . $self->{target} . "\n" if ( notNull($self->{target}) );

    my ($stderr);
    my (@cmd) = ( '-c', 'quit' );

    # no need for stdout
    my ($pid) = $self->spawn_smbclient(undef,\$stderr, @cmd);

    if ( ! $pid || ! $stderr ) {
	$self->print_to_server(sprintf("check smbclient: failed to spawn w/[%s]: %d %s",
                                join("' '",@cmd), ( defined($pid) ? $pid : -1 ), $stderr.""),
                                $Amanda::Script_App::ERROR);
        close($stderr) if ( $stderr );
        unlink($self->{include_filename})
           if ( $self->{include_filename} // 0 );
	return;
    }

    #
    # must close only now (as forked process has copy)
    #
    while (<$stderr>) {
	chomp;
	debug("stderr: " . $_);
	next if /^Domain=/;
	next if /^WARNING/g;
	next if /^Unable to initialize messaging context/g;
	# message if samba server is configured with 'security = share'
	next if /Server not using user level security and no password supplied./;
	$self->print_to_server("smbclient: $_",
			       $Amanda::Script_App::ERROR);
    }
    close($stderr) if ( $stderr );
    waitpid($pid, 0) if ( $pid );
    unlink($self->{tmpfile})
       if ( $self->{tmpfile} // 0 );
    unlink($self->{include_filename})
       if ( $self->{include_filename} // 0 );
    #check statefile
    #check amdevice
}

sub command_estimate {
    my $self = shift;

    $self->parsesharename();
    $self->findpass();
    $self->validate_inexclude();

    my $level = $self->{level}[0];
    my $archive = ( $level ? 1 : 0 );
    my ($pid,$stderr);
    my (@cmd) = ( '-c', "archive ${archive};recurse;du *" );

    $pid = $self->spawn_smbclient(undef, \$stderr, @cmd); # needs '*' for current dir (from {subdir}) and below

    if ( ! $pid || ! $stderr ) {
	$self->print_to_server(sprintf("estimate smbclient: failed to spawn w/[%s]: %d %s",
                                join("' '",@cmd), ( defined($pid) ? $pid : -1 ), $stderr.""),
                                $Amanda::Script_App::ERROR);
        close($stderr) if ( $stderr );
        unlink($self->{include_filename})
           if ( $self->{include_filename} // 0 );
	return;
    }

    my($size) = -1;
    while(<$stderr>) {
	chomp;
	next if /^\s*$/;
	next if /blocks of size/;
	next if /blocks available/;
	next if /^\s*$/;
	next if /^Domain=/;
	next if /dumped \d+ files and directories/;
	next if /^WARNING/g;
	next if /^Unable to initialize messaging context/g;
	# message if samba server is configured with 'security = share'
	next if /Server not using user level security and no password supplied./;
	debug("stderr: $_");
	if ($_ =~ /^Total number of bytes: (\d*)/) {
	    $size = $1;
	    last;
	} else {
	    $self->print_to_server("smbclient: $_",
				   $Amanda::Script_App::ERROR);
	}
    }
    close($stderr);
    waitpid($pid, 0) if ( $pid );
    unlink($self->{tmpfile})
       if ( $self->{tmpfile} // 0 );
    unlink($self->{include_filename})
       if ( $self->{include_filename} // 0 );

    output_size($level, $size);
}

sub output_size
{
   my($level) = shift;
   my($size) = shift;
   my($ksize) = int $size / (1024);

   if($size == -1) {
      print "$level -1 -1\n";
      return;
   }

   $ksize=32 if ($ksize<32);
   print "$level $ksize 1\n";
}

sub send_empty_tar_file {
    my $self = shift;
    my ($out1, $out2) = @_;
    my $out;
    my $buf;
    my $size;

    Amanda::Debug::debug("Create empty archive with: tar --create --file=- --files-from=/dev/null");
    open2($out, undef, "tar", "--create", "--file=-", "--files-from=/dev/null");

    while(($size = sysread($out, $buf, 32768))) {
	syswrite($out1, $buf, $size);
	syswrite($out2, $buf, $size);
    }
}

sub command_backup {
    my $self = shift;

    my $level = $self->{level}[0];
    my ($full_or_inc) = ( $level ? "inc noreset" : "full reset" );

    $self->parsesharename();
    $self->findpass();
    $self->validate_inexclude();

    my ($pid);
    my($smbclient_rdr, $smbclient_err);
    my (@cmd);

    push @cmd, "-d", "0";
    push @cmd, "-c", "";

    $cmd[$#cmd] .= "tarmode $full_or_inc hidden system;"; # quiet
    $cmd[$#cmd] .= " tar qc";
    $cmd[$#cmd] .= "r" if ($self->{regex_match}); # wildcards in the inc/exc patterns
    $cmd[$#cmd] .= "X" if (@{$self->{exclude}}); # not both
    $cmd[$#cmd] .= "I" if (@{$self->{include}}); # not both
    $cmd[$#cmd] .= " - ";
    $cmd[$#cmd] .= " @{$self->{exclude}}" if (@{$self->{exclude}}); # not both
    $cmd[$#cmd] .= " @{$self->{include}}" if (@{$self->{include}}); # not both

    $pid = $self->spawn_smbclient(\$smbclient_rdr, \$smbclient_err, @cmd);

    if ( ! $pid || ! $smbclient_rdr || ! $smbclient_err ) {
	$self->print_to_server(sprintf("backup smbclient: failed to spawn w/[%s]: %d %s %s",
                                join("' '",@cmd), ( defined($pid) ? $pid : -1 ),
                                $smbclient_rdr."", $smbclient_err.""),
                                $Amanda::Script_App::ERROR);
        close($smbclient_rdr) if ( $smbclient_rdr );
        close($smbclient_err) if ( $smbclient_err );
        unlink($self->{include_filename})
           if ( $self->{include_filename} // 0 );
        return;
    }

    #index process
    my ($index_wtr,$index_rdr,$index_err) = (undef, undef, Symbol::gensym);

    debug("$self->{gnutar} -tf -");

    my $pid_index1 = open3($index_wtr, $index_rdr, $index_err,
          $self->{gnutar}, "-tf", "-");
    debug("index " .$index_rdr->fileno);

    my $size = -1;
    my $indexout_fd;
    if (notNull($self->{index})) {
	open($indexout_fd, '>&=4') ||
	    $self->print_to_server_and_die("Can't open indexout_fd: $!",
					   $Amanda::Script_App::ERROR);
    }

    my $file_to_close = 3;
    my $smbclient_stdout_src = Amanda::MainLoop::fd_source($smbclient_rdr,
				$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    my $smbclient_stderr_src = Amanda::MainLoop::fd_source($smbclient_err,
				$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    my $index_tar_stdout_src = Amanda::MainLoop::fd_source($index_rdr,
				$G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    my $index_tar_stderr_src = Amanda::MainLoop::fd_source($index_err,
				$G_IO_IN|$G_IO_HUP|$G_IO_ERR);

    my $smbclient_stdout_done = 0;
    my $smbclient_stderr_done = 0;
    my $data_size = 0;
    my $nb_files = 0;
    $smbclient_stdout_src->set_callback(sub {
	my $buf;
	my $blocksize = -1;
	$blocksize = sysread($smbclient_rdr, $buf, 32768);

	if (!$blocksize) {
	    $file_to_close--;
	    $smbclient_stdout_src->remove();
	    $smbclient_stdout_done = 1;
	    if ($smbclient_stderr_done) {
		if ($data_size == 0 and $nb_files == 0 and $size == 0) {
		    $self->send_empty_tar_file(*STDOUT, $index_wtr);
		}
		close($index_wtr);
		close(STDOUT);
	    }
	    close($smbclient_rdr);
	    Amanda::MainLoop::quit() if $file_to_close == 0;
	    return;
	}

	$data_size += $blocksize;
	syswrite(STDOUT, $buf, $blocksize);
	syswrite($index_wtr, $buf, $blocksize);
    });

    $smbclient_stderr_src->set_callback(sub {
	my $line = <$smbclient_err>;

	if (isNull($line)) {
	    $file_to_close--;
	    $smbclient_stderr_src->remove();
	    $smbclient_stderr_done = 1;
	    if ($smbclient_stdout_done) {
		if ($data_size == 0 and $nb_files == 0 and $size == 0) {
		    $self->send_empty_tar_file(*STDOUT, $index_wtr);
		}
		close($index_wtr);
		close(STDOUT);
	    }
	    close ($smbclient_err);
	    Amanda::MainLoop::quit() if $file_to_close == 0;
	    return;
	}

	chomp $line;
	debug("stderr: " . $line);
	return if $line =~ /Domain=/;
	return if $line =~ /tarmode is now /;
	return if $line =~ /tar_re_search set/;
	return if $line =~ /WARNING/m;
	return if $line =~ /^Unable to initialize messaging context/;
	if ($line =~ /dumped (\d+) files and directories/) {
	    $nb_files = $1;
	    return;
	}
	# message if samba server is configured with 'security = share'
	return if $line =~  /Server not using user level security and no password supplied./;
	if ($line =~ /Total bytes written: (\d*)/) {
	    $size = $1;
	    return;
	} elsif ($line =~ /Total bytes received: (\d*)/) {
	    $size = $1;
	    return;
	}
	$self->print_to_server("smbclient: $line", $Amanda::Script_App::ERROR);
    });

    $index_tar_stdout_src->set_callback(sub {
	my $line = <$index_rdr>;

	if (isNull($line)) {
	    $file_to_close--;
	    $index_tar_stdout_src->remove();
	    close($index_rdr);
	    close($indexout_fd);
	    Amanda::MainLoop::quit() if $file_to_close == 0;
	    return;
	}

	if ($line !~ m{^\./} ) {
	    chomp $line;

	    if ($line =~ /Ignoring unknown extended header keyword/) {
		debug("tar stderr: $line");
                return;
	    }

            $self->print_to_server($line, $Amanda::Script_App::ERROR);
            return;
        }

        return if (isNull($indexout_fd));
        return if (isNull($self->{index}));

        $line =~ s{^\.}{};
        print $indexout_fd $line;
    });

    $index_tar_stderr_src->set_callback(sub {
	my $line = <$index_err>;

	if (isNull($line)) {
	    $file_to_close--;
	    $index_tar_stderr_src->remove();
	    close($index_err);
	    Amanda::MainLoop::quit() if $file_to_close == 0;
	    return;
	}

	chomp $line;
	if ($line =~ /Ignoring unknown extended header keyword/) {
	    debug("tar stderr: $line");
            return;
	}

        $self->print_to_server($line, $Amanda::Script_App::ERROR);
    });

    # handle all "coroutines" together...
    Amanda::MainLoop::run();

    if ($size >= 0) {
	my $ksize = $size / 1024;
	if ($ksize < 32) {
	    $ksize = 32;
	}
	print {$self->{mesgout}} "sendbackup: size $ksize\n";
    }

    waitpid($pid, 0) if ( $pid // 0 );
    unlink($self->{tmpfile})
       if ( $self->{tmpfile} // 0 );
    unlink($self->{include_filename})
       if ( $self->{include_filename} // 0 );

    if ($? != 0) {
	$self->print_to_server_and_die("smbclient returned error",
				       $Amanda::Script_App::ERROR);
        return;
    }
}

sub parse_backup {
    my $self = shift;
    my($fhin, $fhout, $indexout) = @_;
    my $size  = -1;
    while(<$fhin>) {
	if ( /^\.\//) {
	    if(notNull($indexout)) {
		if(notNull($self->{index})) {
		    s/^\.//;
		    print $indexout $_;
		}
	    }
	}
	else {
	    print $fhout "? $_";
	}
    }
}

sub command_index_from_output {
   index_from_output(0, 1);
}

sub index_from_output {
   my($fhin, $fhout) = @_;
   my($size) = -1;
   while(<$fhin>) {
      next if /^Total bytes written:/;
      next if !/^\.\//;
      s/^\.//;
      print $fhout $_;
   }
}

sub command_index {
    my $self = shift;
    my $index_fd;
    open2($index_fd, ">&0", $self->{gnutar}, "--list", "--file", "-") ||
	$self->print_to_server_and_die("Can't run $self->{gnutar}: $!",
				       $Amanda::Script_App::ERROR);
    index_from_output($index_fd, \*STDOUT);
}

sub command_restore {
    my $self = shift;
    my @cmd = ();

    $self->parsesharename();
    chdir(Amanda::Util::get_original_cwd());

    if ($self->{recover_mode} ne "smb")
    {
	qx{mkdir -p $self->{target}};  # try it if possible..
        #
        # sending from archive straight to gnutar...
        #
        push @cmd, $self->{gnutar}, "-xpvf", "-";
        if ($self->{target} // 0) {
            # die if not a writable directory...
            (-d $self->{target} && -w $self->{target})
               || $self->print_to_server_and_die( "Directory $self->{target}: $!", $Amanda::Script_App::ERROR);
            push @cmd, "--directory", $self->{target};
        }

        #
        # use collecting files if needed.. else put on command line
        #
        push @cmd, "--files-from=$_"   for (@{$self->{include_list}});
        push @cmd, "--exclude-from=$_" for (@{$self->{exclude_list}});
        push @cmd, "--exclude=$_"      for (@{$self->{exclude_file}});
        push @cmd, $_                  for (@{$self->{include_file}});

        debug("cmd: '" . join("' '", @cmd) . "'");
        exec { $cmd[0] } @cmd;
        die("Can't exec '", $cmd[0], "'");
        # does not return.. and no filtering of output..
    }

    $self->validate_inexclude();
    $self->findpass();

    if ( $self->{subdir} // 0 ) { 
	my ($oldsubdir) = $self->{subdir};
	$self->{subdir} = "\\";
	my ($pid) = $self->spawn_smbclient(">&STDERR",">&STDERR","-c","mkdir \"${oldsubdir}\"");

	waitpid($pid, 0) if ( $pid // 0 );
	unlink($self->{tmpfile})
	   if ( $self->{tmpfile} // 0 );

	$self->{subdir} = $oldsubdir;
    }

    push @cmd, "-d", "1";
    push @cmd, "-TFx", "-", $self->{include_filename}
        if ($self->{include_filename});

    push @cmd, "-Tx", "-", ( @{$self->{include}}, @::CLI_ARGV )
        if (not $self->{include_filename});

    my ($stderr) = Symbol::gensym;

    my ($pid) = $self->spawn_smbclient(\$stderr,\$stderr,@cmd);

    if ( ! $pid || ! $stderr ) {
        $self->print_to_server(sprintf("restore smbclient: failed to spawn w/[%s]: %d %s",
                                join("' '",@cmd), ( defined($pid) ? $pid : -1 ), $stderr.""),
                                $Amanda::Script_App::ERROR);
        close($stderr) if ( $stderr );
        unlink($self->{include_filename})
           if ( $self->{include_filename} // 0 );
        return;
    }

    while (<$stderr>) {
        chomp;
        next if /^Domain=/;
        next if /^WARNING/m;
        next if /^Unable to initialize messaging context/m;
        debug("stderr: " . $_);
        # message if samba server is configured with 'security = share'
        #next if /Server not using user level security and no password supplied./;
        $self->print_to_server("smbclient: $_",
                               $Amanda::Script_App::ERROR);
    }
    close($stderr);
    waitpid($pid, 0) if ( $pid // 0 );
    unlink($self->{tmpfile})
       if ( $self->{tmpfile} // 0 );
    unlink($self->{include_filename})
       if ( $self->{include_filename} // 0 );
}

sub command_validate {
   my $self = shift;

   if (isNull($self->{gnutar}) || !-x $self->{gnutar}) {
      return $self->default_validate();
   }

   my(@cmd) = ($self->{gnutar}, "-tf", "-");
   debug("cmd:" . join(" ", @cmd));
   my $pid = open3('<&STDIN', '>&STDOUT', '>&STDERR', @cmd) ||
	$self->print_to_server_and_die("Unable to run @cmd: $!",
				       $Amanda::Script_App::ERROR);
   waitpid($pid, 0) if ($pid);
   if( $? != 0 ){
	$self->print_to_server_and_die("$self->{gnutar} returned error",
				       $Amanda::Script_App::ERROR);
   }
}

sub command_print_command {
}

package main;

sub usage {
    print <<EOF;
Usage: amsamba <command> --config=<config> --host=<host> --disk=<disk> --device=<device> --level=<level> --index=<yes|no> --message=<text> --collection=<no> --record=<yes|no> --calcsize.
EOF
    exit(1);
}

my $opt_version;
my $opt_config;
my $opt_host;
my $opt_disk;
my $opt_device;
my @opt_level;
my $opt_index;
my $opt_message;
my $opt_collection;
my $opt_record;
my $opt_calcsize;
my $opt_gnutar_path;
my $opt_smbclient_path;
my $opt_amandapass;
my @opt_exclude_file;
my @opt_exclude_list;
my $opt_exclude_optional;
my @opt_include_file;
my @opt_include_list;
my $opt_include_optional;
my $opt_recover_mode;
my $opt_allow_anonymous;
my $opt_target;
my $opt_regex_match;

sub isNull($) {
    return ! defined($_[0]) || $_[0] eq "";
}
sub notNull($) {
    return defined($_[0]) && $_[0] ne "";
}

Getopt::Long::Configure(qw{bundling});
GetOptions(
    'version'            => \$opt_version,
    'config=s'           => \$opt_config,
    'host=s'             => \$opt_host,
    'disk=s'             => \$opt_disk,
    'device=s'           => \$opt_device,
    'level=s'            => \@opt_level,
    'index=s'            => \$opt_index,
    'message=s'          => \$opt_message,
    'collection=s'       => \$opt_collection,
    'record'             => \$opt_record,
    'calcsize'           => \$opt_calcsize,
    'gnutar-path=s'      => \$opt_gnutar_path,
    'smbclient-path=s'   => \$opt_smbclient_path,
    'amandapass=s'       => \$opt_amandapass,
    'exclude-file=s'     => \@opt_exclude_file,
    'exclude-list=s'     => \@opt_exclude_list,
    'exclude-optional=s' => \$opt_exclude_optional,
    'include-file=s'     => \@opt_include_file,
    'include-list=s'     => \@opt_include_list,
    'include-optional=s' => \$opt_include_optional,
    'recover-mode=s'     => \$opt_recover_mode,
    'allow-anonymous=s'  => \$opt_allow_anonymous,
    'target|directory=s' => \$opt_target,
    'regex-match=s'      => \$opt_regex_match,
) or usage();

if (notNull($opt_version)) {
    print "amsamba-" . $Amanda::Constants::VERSION , "\n";
    exit(0);
}

my $application = Amanda::Application::Amsamba->new($opt_config, $opt_host, $opt_disk, $opt_device, \@opt_level, $opt_index, $opt_message, $opt_collection, $opt_record, $opt_calcsize, $opt_gnutar_path, $opt_smbclient_path, $opt_amandapass, \@opt_exclude_file, \@opt_exclude_list, $opt_exclude_optional, \@opt_include_file, \@opt_include_list, $opt_include_optional, $opt_recover_mode, $opt_allow_anonymous, $opt_target, $opt_regex_match);

Amanda::Debug::debug("Arguments: [$::ARGV[0]]" . join(' ', @Amanda::Application::Amsamba::CLI_ARGV));

$application->do($ARGV[0]);
# NOTREACHED
