# vim:ft=perl
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

package Installcheck::Rest;

use Amanda::Paths;
use Amanda::Debug;
use WWW::Curl::Easy;
use JSON;
use Test::More;

my $dance_name;
eval { require Dancer2; };
if (!$@) {
    $dance_name = "$Amanda::Paths::amlibexecdir/rest-server/bin/app-dancer2.pl";
} else {
    Amanda::Debug::debug("Failed to load Dancer2: $@");
    eval { require Dancer; };
    if (!$@) {
	$dance_name = "$Amanda::Paths::amlibexecdir/rest-server/bin/app.pl";
    } else {
	Amanda::Debug::debug("Failed to load Dancer: $@");
	die("Can't load dancer or Dancer2");
    }
}

if ($WWW::Curl::Easy::VERSION < 4.14) {
    Amanda::Debug::debug("WWW::Curl is too old");
    die("WWW::Curl is too old");
}

=head1 NAME

Installcheck::Rest - utilities to start/stop the Rest server.

=head1 SYNOPSIS

  use Installcheck::Rest;

  # start the Rest server.
  Installcheck::Rest::start();

  #make request to the Rest server.
  # ...

  # stop the Rest server.
  Installcheck::Rest::stop();

=cut

sub DESTROY {
    stop();
}

sub new {
    my $class = shift;

    my $curl = WWW::Curl::Easy->new;
    if (!defined $curl) {
	die("Can't run curl");
    }
    my $json = JSON->new->allow_nonref;

    my $pid = fork;
    if ($pid == 0) {
	Amanda::Debug::debug_dup_stderr_to_debug();
	exec("starman", "--env", "development", "--port", "5001",
			"--workers", "1", $dance_name);
	exit(-1);
    } elsif ($pid < 0) {
	die("Can't fork for rest server");
    }

    my $self = {
	pid  => $pid,
	curl => $curl,
	json => $json
    };
    bless $self, $class;

    # Wait for the server to start
    my $time = time();
    my $retcode = -1;
    while ($retcode != 0) {
	$curl->setopt(CURLOPT_HEADER, 0);
	$curl->setopt(CURLOPT_URL, "http://localhost:5001/amanda/h1/v1.0");
	$curl->setopt(CURLOPT_POST, 0);

	my $response_body;
	$curl->setopt(CURLOPT_WRITEDATA,\$response_body);
	$retcode = $curl->perform;

	if ($retcode !=0 and time() > $time + 5) {
	    kill 'TERM', $pid;

	    $self->{'error'} = "Can't connect to the Rest server.";
	    return $self;
	}
    }

    use POSIX ":sys_wait_h";
    my $kid = waitpid($pid, WNOHANG);

    if ($kid == $pid) {
	$self->{'error'} = $kid;
    }
    return $self;
}

sub stop {
    my $self = shift;

    if (defined $self->{'pid'}) {
	kill 'TERM', $self->{'pid'};
	$self->{'pid'} = undef;
    }
}

sub get {
    my $self = shift;
    my $url = shift;

    $self->{'curl'}->setopt(CURLOPT_HEADER, 0);
    $self->{'curl'}->setopt(CURLOPT_URL, $url);
    $self->{'curl'}->setopt(CURLOPT_POST, 0);

    my $response_body;
    $self->{'curl'}->setopt(CURLOPT_WRITEDATA,\$response_body);

    my $retcode = $self->{'curl'}->perform;

    if ($retcode == 0) {
	return {
	    http_code => $self->{'curl'}->getinfo(CURLINFO_HTTP_CODE),
	    body      => $self->{'json'}->decode($response_body),
	};
    } else {
	die("An error happened: $retcode ".$self->{'curl'}->strerror($retcode)." ".$self->{'curl'}->errbuf."\n");
    }
}

sub post {
    my $self = shift;
    my $url = shift;
    my $postfields = shift;

    $self->{'curl'}->setopt(CURLOPT_HEADER, 0);
    $self->{'curl'}->setopt(CURLOPT_URL, $url);
    $self->{'curl'}->setopt(CURLOPT_POST, 1);
    $self->{'curl'}->setopt(CURLOPT_POSTFIELDS, $postfields);
    $self->{'curl'}->setopt(CURLOPT_POSTFIELDSIZE, length($postfields) || 0);
    $self->{'curl'}->setopt(CURLOPT_HTTPHEADER, [
		"Content-Type: application/json; charset=utf-8" ]);

    my $response_body;
    $self->{'curl'}->setopt(CURLOPT_WRITEDATA,\$response_body);

    my $retcode = $self->{'curl'}->perform;

    if ($retcode == 0) {
	my $http_code = $self->{'curl'}->getinfo(CURLINFO_HTTP_CODE);
	if ($http_code >= 500) {
	    return {
		http_code => $http_code,
	    };
	} else {
	    return {
		http_code => $http_code,
		body      => $self->{'json'}->decode($response_body),
	    };
	}
    } else {
	die("An error happened: $retcode ".$self->{'curl'}->strerror($retcode)." ".$self->{'curl'}->errbuf."\n");
    }
}

sub delete {
    my $self = shift;
    my $url = shift;
    my $postfields = shift;

    $self->{'curl'}->setopt(CURLOPT_HEADER, 0);
    $self->{'curl'}->setopt(CURLOPT_URL, $url);
    $self->{'curl'}->setopt(CURLOPT_CUSTOMREQUEST, 'DELETE');
    #$self->{'curl'}->setopt(CURLOPT_POST, 1);
    #$self->{'curl'}->setopt(CURLOPT_POSTFIELDS, $postfields);
    #$self->{'curl'}->setopt(CURLOPT_INFILESIZE_LARGE, 0);
    #$self->{'curl'}->setopt(CURLOPT_HTTPHEADER, [
#		"Content-Type: application/json; charset=utf-8" ]);

    my $response_body;
    $self->{'curl'}->setopt(CURLOPT_WRITEDATA,\$response_body);

    my $retcode = $self->{'curl'}->perform;

    if ($retcode == 0) {
	my $http_code = $self->{'curl'}->getinfo(CURLINFO_HTTP_CODE);
	if ($http_code >= 500) {
	    return {
		http_code => $http_code,
	    };
	} else {
	    return {
		http_code => $http_code,
		body      => $self->{'json'}->decode($response_body),
	    };
	}
    } else {
	die("An error happened: $retcode ".$self->{'curl'}->strerror($retcode)." ".$self->{'curl'}->errbuf."\n");
    }
}

sub cleanup_for_amdump {
    my $reply = shift;
    my $body = $reply->{'body'};

    if ($body->[0]{report}{notes}[0] =~ /amdump: fork amdump/) {
	shift @{$body->[0]{report}{notes}};
    }
    if ($body->[0]{report}{head}) {
	$body->[0]{report}{head}->{date} = undef;
	$body->[0]{report}{head}->{hostname} = undef;
    }
    if ($body->[0]{report}{statistic}) {
	$body->[0]{report}{statistic}{estimate_time} = undef;
	$body->[0]{report}{statistic}{run_time} = undef;
	$body->[0]{report}{statistic}{dump_time} = undef;
	$body->[0]{report}{statistic}{tape_time} = undef;
	$body->[0]{report}{statistic}{avg_dump_rate} = undef;
	$body->[0]{report}{statistic}{Avg_tape_write_speed} = undef;
    }

    if (exists $body->[0]{report}{summary}) {
	foreach my $summary (@{$body->[0]{report}{summary}}) {
	    $summary->{dump_duration} = undef;
	    $summary->{dump_rate} = undef;
	    $summary->{tape_duration} = undef;
	    $summary->{tape_rate} = undef;
	}
    }

    if (exists $body->[0]{report}{usage_by_tape}) {
	foreach my $usage_by_tape (@{$body->[0]{report}{usage_by_tape}}) {
	    $usage_by_tape->{time_duration} = undef;
	}
    }

    if (exists $body->[0]{report}{failure_details}) {
	foreach my $failure_detail (@{$body->[0]{report}{failure_details}}) {
	    $failure_detail =~ s/\.\d*\.debug/.DATESTAMP.debug/g;
	}
    }

    return $reply;
}

1;
