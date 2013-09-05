# Copyright (c) 2012 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

use lib '@amperldir@';

use strict;
use warnings;

use JSON -convert_blessed_universally;

use Amanda::JSON::RPC::Dispatcher;
use Amanda::Debug;
use Amanda::Changer;
use Amanda::Config;
use Amanda::Constants;
use Amanda::Device;
use Amanda::Disklist;
use Amanda::Tapelist;
use Amanda::Feature;
use Amanda::Header;
use Amanda::Holding;
use Amanda::Interactivity;
use Amanda::MainLoop;
use Amanda::Paths;
use Amanda::Process;
use Amanda::Util qw( :constants );
use Amanda::JSON::Amdump;
use Amanda::JSON::Config;
use Amanda::JSON::Changer;
use Amanda::JSON::DB::Catalog;
use Amanda::JSON::Device;
use Amanda::JSON::Dle;
use Amanda::JSON::Label;
use Amanda::JSON::Report;
use Amanda::JSON::Status;
use Amanda::JSON::Tapelist;

Amanda::Util::setup_application("amjson-server", "server", $CONTEXT_CMDLINE);
Amanda::Config::config_init(0,undef);
Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

use Data::Dumper;

my $rpc = Amanda::JSON::RPC::Dispatcher->new;


$rpc->register( 'Amanda::JSON::Amdump::run', \&Amanda::JSON::Amdump::run );

$rpc->register( 'Amanda::JSON::Config::getconf_byname', \&Amanda::JSON::Config::getconf_byname );
$rpc->register( 'Amanda::JSON::Config::config_dir_relative', \&Amanda::JSON::Config::config_dir_relative );

$rpc->register( 'Amanda::JSON::Changer::inventory', \&Amanda::JSON::Changer::inventory );
$rpc->register( 'Amanda::JSON::Changer::load', \&Amanda::JSON::Changer::load );
$rpc->register( 'Amanda::JSON::Changer::reset', \&Amanda::JSON::Changer::reset );
$rpc->register( 'Amanda::JSON::Changer::eject', \&Amanda::JSON::Changer::eject );
$rpc->register( 'Amanda::JSON::Changer::clean', \&Amanda::JSON::Changer::clean );
$rpc->register( 'Amanda::JSON::Changer::verify', \&Amanda::JSON::Changer::verify );
$rpc->register( 'Amanda::JSON::Changer::show', \&Amanda::JSON::Changer::show );
$rpc->register( 'Amanda::JSON::Changer::label', \&Amanda::JSON::Changer::label );
$rpc->register( 'Amanda::JSON::Changer::update', \&Amanda::JSON::Changer::update );

$rpc->register( 'Amanda::JSON::DB::Catalog::get_parts', \&Amanda::JSON::DB::Catalog::get_parts );
$rpc->register( 'Amanda::JSON::DB::Catalog::get_dumps', \&Amanda::JSON::DB::Catalog::get_dumps );

$rpc->register( 'Amanda::JSON::Device::read_label', \&Amanda::JSON::Device::read_label );
$rpc->register( 'Amanda::JSON::Device::get_properties', \&Amanda::JSON::Device::get_properties );

$rpc->register( 'Amanda::JSON::Dle::force', \&Amanda::JSON::Dle::force );
$rpc->register( 'Amanda::JSON::Dle::force_level_1', \&Amanda::JSON::Dle::force_level_1 );
$rpc->register( 'Amanda::JSON::Dle::unforce', \&Amanda::JSON::Dle::unforce );
$rpc->register( 'Amanda::JSON::Dle::force_bump', \&Amanda::JSON::Dle::force_bump );
$rpc->register( 'Amanda::JSON::Dle::force_no_bump', \&Amanda::JSON::Dle::force_no_bump );
$rpc->register( 'Amanda::JSON::Dle::unforce_bump', \&Amanda::JSON::Dle::unforce_bump );

$rpc->register( 'Amanda::JSON::Label::assign', \&Amanda::JSON::Label::assign );
$rpc->register( 'Amanda::JSON::Label::label', \&Amanda::JSON::Label::label );
$rpc->register( 'Amanda::JSON::Label::erase', \&Amanda::JSON::Label::erase );
$rpc->register( 'Amanda::JSON::Label::reuse', \&Amanda::JSON::Label::reuse );
$rpc->register( 'Amanda::JSON::Label::no_reuse', \&Amanda::JSON::Label::no_reuse );

$rpc->register( 'Amanda::JSON::Report::report', \&Amanda::JSON::Report::report );

$rpc->register( 'Amanda::JSON::Status::current', \&Amanda::JSON::Status::current );

$rpc->register( 'Amanda::JSON::Tapelist::get', \&Amanda::JSON::Tapelist::get );

$rpc->to_app;
