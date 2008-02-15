#!/usr/bin/perl -w
# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use HTML::Form;
use LWP::UserAgent;
#use LWP::Debug qw(+ +conns);

use strict;

die "USAGE: $0 [login] [password]\n" unless @ARGV == 2;

# Authentication details
my ($login, $password) = @ARGV;

# Page parsing details
my $url = 'http://aws-portal.amazon.com/gp/aws/user/subscription/index.html?offeringCode=3423231F';
my $email_field = 'email';
my $pass_field = 'password';
my $radio_field = 'action';
my $radio_value = 'sign-in';

# Configuration ends here.
my $ua = LWP::UserAgent->new;
$ua->agent("Zmanda activation key retreival tool/1.0");
$ua->cookie_jar({});
# Amazon doesn't follow spec, what a surprise.
$ua->requests_redirectable( [ 'GET', 'HEAD', 'POST' ]);

my $formreq = HTTP::Request->new(GET => $url);

my $formres = $ua->request($formreq);

if (!$formres->is_success) {
    die $formres->status_line . "\n";
}

my @forms =
    grep { defined $_->find_input($email_field) &&
           defined $_->find_input($pass_field) }
             HTML::Form->parse($formres);

die "Could not find form.\n" unless @forms == 1;

my $form = shift @forms;

$form->value($email_field, $login);
$form->value($pass_field, $password);
$form->value($radio_field, $radio_value);

my $finalres = $ua->request($form->click);

if (!$finalres->is_success) {
    die $finalres->status_line . "\n";
}

$finalres->decoded_content() =~ /ActivationKey=([A-Z0-9]+)/;
die "Can't find activation key in output.\n" unless defined $1;

print "$1\n";
