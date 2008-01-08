#!/usr/bin/perl -w

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
