#!/usr/bin/perl

# re-exec using the PERL environment var if different executible
if ( $ENV{PERL} && -x $ENV{PERL} && ! $ENV{PERL_REEXEC} ) {
    my $inode = (stat($ENV{PERL}))[1];
    my $curinode = (stat($^X))[1];
    $ENV{PERL_REEXEC} = $ENV{PERL};
    exec($ENV{PERL},$0,@ARGV) if ($inode != $curinode);
}

# in case sub-env. is used in a new executible
delete $ENV{PERL_REEXEC};

# likely default ones and then more critical extra ones
my @modules = qw{
    Carp Cwd Data::Dumper Errno Fcntl File::Find File::Path IO::File IO::Handle 
       IO::Pipe IO::Socket::INET IO::Socket::UNIX IO::Socket IPC::Open3 Math::BigInt 
       POSIX Socket Symbol Sys::Hostname 
    Test::More parent WWW::Curl::Easy Test::Harness JSON };
push @modules,@ARGV;  # add on deps if needed...

my $failed = 0;

for $i ( @modules ) {
    eval "require $i;"; $@ && ++$failed && print "cannot require $i\n";
}

sub test_port($) {
    my $port = $_[0];
    my $sock = IO::Socket::INET->new(LocalAddr => "127.0.0.1:$port", Listen => 10); 
    my $sock2 = IO::Socket::INET->new("127.0.0.1:$port"); 
    return 0 unless ( $sock && $sock2->connected() );

    $sock = $sock->accept(); 
    return 0 unless ( $sock );
    # detects accept
    $sock2->write($port,1) || return 0;
    $sock2->shutdown(2) || return 0;
    # detects connector
    $sock->read($port,1) || return 0;
    $sock->shutdown(2) || return 0;
    return 1;
}

test_port(10000) && print("connected on 10000\n") || ++$failed;

$failed || print "all are available\n";
$failed && print "$failed tests failed\n";

exit($failed);
