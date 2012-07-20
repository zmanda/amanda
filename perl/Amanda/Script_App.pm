# vim:ft=perl
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Script_App;

no warnings;
no strict;
$GOOD  = 0;
$ERROR = 1;
$FAILURE = 2;

use strict;
use warnings;
use Amanda::Constants;
use Amanda::Config qw( :init :getconf  config_dir_relative );
use Amanda::Debug qw( :logging );
use Amanda::Paths;
use Amanda::Util qw( :constants );
use Carp;

=head1 NAME

Amanda::Script_App - perl utility functions for Scripts.

=head1 SYNOPSIS

This module should not be used directly. Instead, use C<Amanda::Application> or
C<Amanda::Script>.

=cut

sub new {
    my $class = shift;
    my ($execute_where, $type, $config_name) = @_;

    my $self = {};
    bless ($self, $class);

    # extract the last component of the class name
    my $name = $class;
    $name =~ s/^.*:://;
    $self->{'name'} = $name;

    if(!defined $execute_where) {
	$execute_where = "client";
    }
    Amanda::Util::setup_application($name, $execute_where, $CONTEXT_DAEMON);
    debug("Arguments: " . join(' ', @ARGV));

    #initialize config client to get values from amanda-client.conf
    config_init($CONFIG_INIT_CLIENT, undef);
    my ($cfgerr_level, @cfgerr_errors) = config_errors();
    if ($cfgerr_level >= $CFGERR_WARNINGS) {
        config_print_errors();
        if ($cfgerr_level >= $CFGERR_ERRORS) {
            confess("errors processing config file");
        }
    }
    if ($config_name) {
        config_init($CONFIG_INIT_CLIENT | $CONFIG_INIT_EXPLICIT_NAME | $CONFIG_INIT_OVERLAY, $config_name);
        ($cfgerr_level, @cfgerr_errors) = config_errors();
        if ($cfgerr_level >= $CFGERR_WARNINGS) {
            config_print_errors();
            if ($cfgerr_level >= $CFGERR_ERRORS) {
                confess("errors processing config file for $config_name");
            }
        }
    }

    Amanda::Util::finish_setup($RUNNING_AS_ANY);

    $self->{error_status} = $Amanda::Script_App::GOOD;
    $self->{type} = $type;
    $self->{known_commands} = {};

    debug("$type: $name\n");

    return $self;
}


#$_[0] message
#$_[1] status: GOOD or ERROR
sub print_to_server {
    my $self = shift;
    my($msg, $status) = @_;
    if ($status != 0) {
        $self->{error_status} = $status;
    }
    if ($self->{action} eq "check") {
	if ($status == $Amanda::Script_App::GOOD) {
            print STDOUT "OK $msg\n";
	} else {
            print STDOUT "ERROR $msg\n";
	}
    } elsif ($self->{action} eq "estimate") {
	if ($status == $Amanda::Script_App::GOOD) {
            #do nothing
	} else {
            print STDERR "ERROR $msg\n";
	}
    } elsif ($self->{action} eq "backup") {
	if ($status == $Amanda::Script_App::GOOD) {
            print {$self->{mesgout}} "| $msg\n";
	} elsif ($status == $Amanda::Script_App::ERROR) {
            print {$self->{mesgout}} "? $msg\n";
	} else {
            print {$self->{mesgout}} "sendbackup: error $msg\n";
	}
    } elsif ($self->{action} eq "restore") {
        print STDERR "$msg\n";
    } elsif ($self->{action} eq "validate") {
        print STDERR "$msg\n";
    } else {
        print STDERR "$msg\n";
    }
}

#$_[0] message
#$_[1] status: GOOD or ERROR
sub print_to_server_and_die {
    my $self = shift;

    $self->print_to_server( @_ );
    if (!defined $self->{die} && $self->can("check_for_backup_failure")) {
	$self->{die} = 1;
	$self->check_for_backup_failure();
    }
    exit 1;
}


sub do {
    my $self = shift;
    my $command  = shift;

    if (!defined $command) {
	$self->print_to_server_and_die("check", "no command",
				       $Amanda::Script_App::ERROR);
	return;
    }
    $command =~ tr/A-Z-/a-z_/;
    debug("command: $command");

    # first make sure this is a valid command.
    if (!exists($self->{known_commands}->{$command})) {
	print STDERR "Unknown command `$command'.\n";
	exit 1;
    }

    my $action = $command;
    $action =~ s/^pre_//;
    $action =~ s/^post_//;
    $action =~ s/^inter_//;
    $action =~ s/^dle_//;
    $action =~ s/^host_//;
    $action =~ s/^level_//;

    if ($action eq 'amcheck' || $action eq 'selfcheck') {
	$self->{action} = 'check';
    } elsif ($action eq 'estimate') {
	$self->{action} = 'estimate';
    } elsif ($action eq 'backup') {
	$self->{action} = 'backup';
    } elsif ($action eq 'recover' || $action eq 'restore') {
	$self->{action} = 'restore';
    } elsif ($action eq 'validate') {
	$self->{action} = 'validate';
    }

    if ($action eq 'backup') {
	$self->_set_mesgout();
    }

    # now convert it to a function name and see if it's
    # defined
    my $function_name = "command_$command";
    my $default_name = "default_$command";

    if (!$self->can($function_name)) {
        if (!$self->can($default_name)) {
            print STDERR "command `$command' is not supported by the '" .
                         $self->{name} . "' " . $self->{type} . ".\n";
            exit 1;
	}
	$self->$default_name();
	return;
    }

    # it exists -- call it
    $self->$function_name();
}

1;
