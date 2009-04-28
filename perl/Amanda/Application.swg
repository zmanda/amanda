/*
 * Copyright (c) Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

%module "Amanda::Application"
%include "amglue/amglue.swg"
%include "exception.i"
%include "cstring.i"

%perlcode %{
push @ISA, qw(Amanda::Script_App);
require Amanda::Script_App;

use strict;
use warnings;
use Amanda::Config qw( :init :getconf  config_dir_relative );

=head1 NAME

Amanda::Application - perl utility functions for Applications.

=head1 SYNOPSIS

  package Amanda::Application::my_application;
  use base qw(Amanda::Application);

  sub new {
    my ($class, $config, $foo) = @_;
    my $self = $class->SUPER::new($config);

    $self->{'foo'} = $foo;
    $self->{'bar'} = $bar;

    return $self;
  }

  # Define all command_* subs that you need, e.g.,
  sub command_support {
    my $self = shift;
    # ...
  }

  package main;

  # .. parse arguments ..

  my $application = Amanda::Application::my_application->new($opt_foo, $opt_bar);
  $application->do($cmd);

=cut

sub new {
    my $class = shift @_;
    my $config_name = shift @_;

    my $self = Amanda::Script_App::new($class, "client", "application", $config_name);

    $self->{known_commands} = {
        support   => 1,
        selfcheck => 1,
        estimate  => 1,
        backup    => 1,
        restore   => 1,
        validate  => 1,
    };
    return $self;
}

sub run_calcsize {
    my $self = shift;
    my $program = shift;

    run_calcsize_C($self->{config}, $program, $self->{disk}, $self->{device}, $self->{level}, undef, undef);

}

sub default_validate {
    my $self = shift;

    while (<STDIN>) {}
}
%}

/* C interfaces used by the above */

%{
#include "amanda.h"
#include "client_util.h"
%}

%typemap(in) GSList *levels {
    AV     *tempav;
    GSList *level = NULL;
    int     num;
    int     i;
    SV    **tv;

    if (!SvROK($input))
	croak("Argument $argnum is not a reference.");
    if (SvTYPE(SvRV($input)) != SVt_PVAV)
	croak("Argument $argnum is not an array.");
    tempav = (AV*)SvRV($input);
    num = av_len(tempav);
    for (i=0; i <= num; i++) {
	tv = av_fetch(tempav, i, 0);
	level = g_slist_append(level, GINT_TO_POINTER(SvIV(*tv)));
    }
    $1 = level;
}
/* free the list */
%typemap(freearg) GSList *levels {
    if($1)
	g_slist_free($1);
}

%rename(run_calcsize_C) run_calcsize;
void
run_calcsize(char *config, char *program, char *disk, char *dirname,
	     GSList *levels, char *file_exclude, char *file_include);

%typemap(in) GSList *levels;
%typemap(freearg) GSList *levels;
