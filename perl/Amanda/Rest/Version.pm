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

package Amanda::Rest::Version;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Constants;
use Symbol;
use Data::Dumper;
use Scalar::Util;
use vars qw(@ISA);
use Amanda::Debug;
use Amanda::Util qw( :constants );

=head1 NAME

Amanda::Rest::Version

=head1 INTERFACE

=cut

sub version {
    my %params = @_;
    my @result_messages;

    push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code     => 1550000,
				VERSION  => $Amanda::Constants::VERSION,
				BUILT_DATE => $Amanda::Constants::BUILT_DATE,
				BUILT_REV => $Amanda::Constants::BUILT_REV,
				BUILT_BRANCH => $Amanda::Constants::BUILT_BRANCH);
    return \@result_messages;
}


1;
