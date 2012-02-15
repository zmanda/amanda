#!/usr/bin/env perl
use strict;
use warnings;

use constant DATE_FMT => '%04d-%02d-%02d';

my @now = localtime(time);
my $old = sprintf(DATE_FMT, 1900+$now[5]-2, 1+$now[4], $now[3]);
my $cur_date = sprintf(DATE_FMT, 1900+$now[5], 1+$now[4], $now[3]);
my %authors;

open LOG, "< ChangeLog";
while (my $l = <LOG>) {
    chomp $l;
    if ($l =~ /(\d+)-(\d+)-(\d+)\s+(.*)/) {
        my ($year, $mon, $day, $auth_part) = ($1, $2, $3, $4);
        my $d = sprintf(DATE_FMT, $year, $mon, $day);
        if ($cur_date lt $d) {
            print "warning: out-of-order entry: $l\n";
        }
        $cur_date = $d;
        if ($d gt $old) {
            foreach my $a (split /,\s+/, $auth_part) {
                $a =~ s/,\s+//;
                $authors{$a}++;
            }
        } else {
            last;
        }
    }
}

print (join("\n", sort(map {"$authors{$_}\t$_"} keys(%authors))) . "\n");
