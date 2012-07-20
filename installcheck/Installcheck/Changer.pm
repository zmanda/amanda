# vim:ft=perl
# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Installcheck::Changer;

=head1 NAME

Installcheck::Changer - utilities for testing changers

=head1 SYNOPSIS

  use Installcheck::Changer;

  my $res_cb = sub {
    my ($err, $res) = @_;
    chg_err_like($err,
	{ message => "expected msg", type => 'failure' },
	"operation produces the expected error");
    # or, just looking at the message
    chg_err_like($err,
	qr/expected .*/,
	"operation produces the expected error");
  };

=head1 USAGE

The function C<chg_err_like> takes an C<Amanda::Changer::Error> object and a
hashref of expected values for that error object, and compares the two.  The
values of this hashref can be regular expressions or strings.  Alternately, the
function can take a regexp which is compared to the error's message.  This
function is exported by default.

=cut

use Test::More;
use Data::Dumper;
use strict;
use warnings;
use vars qw( @ISA @EXPORT );

require Exporter;
@ISA = qw(Exporter);
@EXPORT = qw(chg_err_like);

sub chg_err_like {
    my ($err, $expected, $msg) = @_;

    if (!defined($err) or !$err->isa("Amanda::Changer::Error")) {
	fail($msg);
	diag("Expected an Amanda::Changer::Error object; got\n" . Dumper($err));
	return;
    }

    if (ref($expected) eq 'Regexp') {
	like($err->{'message'}, $expected, $msg);
    } else {
	my $ok = 1;
	for my $key (qw( type reason message )) {
	    if (exists $expected->{$key}) {
		if (!exists $err->{$key}) {
		    fail($msg) if ($ok);
		    $ok = 0;
		    diag("expected a '$key' hash elt, but saw none");
		    next;
		}

		my ($got, $exp) = ($err->{$key}, $expected->{$key});
		if (ref($exp) eq "Regexp") {
		    if ($got !~ $exp) {
			fail($msg) if $ok;
			$ok = 0;
			diag("$key '$got' does not match '$exp'");
		    }
		} elsif ($got ne $exp) {
		    fail($msg) if ($ok);
		    $ok = 0;
		    diag("expected $key '$exp'; got $key '$got'");
		}
	    }
	}
	pass($msg) if ($ok);
    }
}
