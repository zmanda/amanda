#! @PERL@
# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

package main;

use Amanda::Debug qw( debug );
use Amanda::Util qw( :constants );
use Amanda::Config qw( :init );
use Amanda::Service::Restore;

our $exit_status = 0;

Amanda::Util::setup_application("restore", "client", $CONTEXT_DAEMON, "amanda", "amanda");
config_init($CONFIG_INIT_GLOBAL, undef);

my $restore = Amanda::Service::Restore->new();
Amanda::MainLoop::call_later(sub { $restore->run(); });
Amanda::MainLoop::run();

debug("exiting with $exit_status");
Amanda::Util::finish_application();

exit($exit_status);
