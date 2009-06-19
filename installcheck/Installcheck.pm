# vim:ft=perl
# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Installcheck;
use File::Path;
use Amanda::Paths;

=head1 NAME

Installcheck - utilities for installchecks (not installed)

=head1 SYNOPSIS

  use Installcheck;

  my $testdir = "$Installcheck::TMP/mystuff/";

=head1 DESCRIPTION

This module defines C<$TMP>, the temporary directory for installcheck data.

=cut

no warnings;

$TMP = "$AMANDA_TMPDIR/installchecks";
mkpath($TMP);

1;
