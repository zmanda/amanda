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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 22;
use strict;
use warnings;

use lib "@amperldir@";
use Amanda::Config::FoldingHash;
use Data::Dumper;

my $h = Amanda::Config::FoldingHash->new();
ok(!(exists $h->{'key'}), "key doesn't exist in new hash");
is_deeply([sort(keys(%$h))], [], "hash starts out with no keys");

$h->{'key'} = 3;
ok((exists $h->{'key'}), "key exists after assignment");
is($h->{'key'}, 3, "can fetch value stored in simple key");
is($h->{'kEy'}, 3, "can fetch value stored in key via fold");

$h->{'key'} = 4;
is($h->{'key'}, 4, "updating the one key works");
is_deeply([sort(keys(%$h))], [sort(qw(key))], "hash now has the one key");

delete $h->{'key'};
ok(!(exists $h->{'key'}), "key doesn't exist after deletion");
is($h->{'key'}, undef, "got undef fetching deleted key");

$h->{'kEY'} = 20;
ok((exists $h->{'kEY'}), "unfolded key exists after assignment");
ok((exists $h->{'key'}), "folded key exists after assignment");
is($h->{'key'}, 20, "can fetch value stored in folded key");
is_deeply([sort(keys(%$h))], [sort(qw(key))], "key is folded in list");

$h->{'key'} = 92;
is($h->{'key'}, 92, "updated folded key");

$h->{'Key'} = 37;
is($h->{'key'}, 37, "updated key via fold");

$h->{'_some-OTHER_kEy_'} = undef;
ok((exists $h->{'_some-OTHER_kEy_'}), "longer key exists after assigning undef");
ok((exists $h->{'-some-other-key-'}), "longer folded key exists too");
ok((exists $h->{'-SOME_other_key_'}), "longer key exists via fold");
is_deeply([sort(keys(%$h))], [sort(qw(key -some-other-key-))], "keys list as folded");

delete $h->{'KeY'};
ok(!(exists $h->{'key'}), "key doesn't exist after deletion via fold");
is($h->{'key'}, undef, "got undef fetching folded deleted key");

for my $k (qw(_ __ a-B Cf_ __g-h i__j-k L)) {
    $h->{$k} = $k;
}

is_deeply([sort(keys(%$h))], [sort(qw(-some-other-key- - -- a-b cf- --g-h i--j-k l))],
   "various keys are listed as folded");

