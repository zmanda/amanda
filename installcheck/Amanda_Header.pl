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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 55;
use strict;
use warnings;

use lib "@amperldir@";
use Amanda::Header;
use Amanda::Debug;
use Installcheck;

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# Test all of the setters and getters, as well as to_string and from_string, by
# constructing a header and converting it to a string and back.  This is a little
# tricky, because of the various restrictions on combinations of fields in the
# header, so we use multple headers.

my $hdr1 = Amanda::Header->new();
$hdr1->{'type'} = $Amanda::Header::F_DUMPFILE;
$hdr1->{'datestamp'} = '20090102030405';
$hdr1->{'dumplevel'} = 6;
$hdr1->{'compressed'} = 1;
$hdr1->{'encrypted'} = 1;
$hdr1->{'comp_suffix'} = '.TEENSY';
$hdr1->{'encrypt_suffix'} = 'E';
$hdr1->{'name'} = 'schlitz';
$hdr1->{'disk'} = '/pbr';
$hdr1->{'program'} = 'GNUTAR';
$hdr1->{'application'} = 'amnot';
$hdr1->{'srvcompprog'} = 'scp';
$hdr1->{'srv_encrypt'} = 'se';
$hdr1->{'recover_cmd'} = 'rec |';
$hdr1->{'uncompress_cmd'} = 'unc |';
$hdr1->{'decrypt_cmd'} = 'dec |';
$hdr1->{'srv_decrypt_opt'} = '-dos';
$hdr1->{'cont_filename'} = '/path/to/cont';
$hdr1->{'dle_str'} = "D\nL\nE";
$hdr1->{'is_partial'} = 1;
$hdr1->{'blocksize'} = 32000;
$hdr1->{'orig_size'} = 10240;

my $hdr2 = Amanda::Header->new();
$hdr2->{'type'} = $Amanda::Header::F_SPLIT_DUMPFILE;
$hdr2->{'datestamp'} = '20090102030405';
$hdr2->{'dumplevel'} = 6;
$hdr2->{'compressed'} = 1;
$hdr2->{'encrypted'} = 1;
$hdr2->{'comp_suffix'} = '.TEENSY';
$hdr2->{'encrypt_suffix'} = 'E';
$hdr2->{'name'} = 'schlitz';
$hdr2->{'disk'} = '/pbr';
$hdr2->{'program'} = 'GNUTAR';
$hdr2->{'application'} = 'amnot';
$hdr2->{'clntcompprog'} = 'ccp';
$hdr2->{'clnt_encrypt'} = 'ce';
$hdr2->{'recover_cmd'} = 'rec |';
$hdr2->{'uncompress_cmd'} = 'unc |';
$hdr2->{'decrypt_cmd'} = 'dec |';
$hdr2->{'clnt_decrypt_opt'} = '-doc';
$hdr2->{'cont_filename'} = '/path/to/cont';
$hdr2->{'dle_str'} = "D\nL\nE";
$hdr2->{'is_partial'} = 1;
$hdr2->{'partnum'} = 13;
$hdr2->{'totalparts'} = 14;
$hdr2->{'blocksize'} = 32000;
$hdr2->{'orig_size'} = 10240;

my $hdr3 = Amanda::Header->new();
$hdr3->{'type'} = $Amanda::Header::F_TAPESTART;
$hdr3->{'name'} = 'TAPE17';
$hdr3->{'datestamp'} = '20090102030405';

my $string1 = $hdr1->to_string(32768, 32768);
my $string2 = $hdr2->to_string(65536, 65536);
my $string3 = $hdr3->to_string(32768, 32768);

is(length($string1), 32768, "generated header 1 has correct length");
is(length($string2), 65536, "generated header 2 has correct length");

like($string3,
     qr/^AMANDA: TAPESTART DATE 20090102030405 TAPE TAPE17/,
     "generated tapestart header looks OK");

$hdr1 = Amanda::Header->from_string($string1);
$hdr2 = Amanda::Header->from_string($string2);
$hdr3 = Amanda::Header->from_string($string3);

is($hdr1->{'type'}, $Amanda::Header::F_DUMPFILE, "'type' for hdr1");
is($hdr1->{'datestamp'}, '20090102030405', "'datestamp' for hdr1");
is($hdr1->{'dumplevel'}, 6, "'dumplevel' for hdr1");
is($hdr1->{'compressed'}, 1, "'compressed' for hdr1");
is($hdr1->{'encrypted'}, 1, "'encrypted' for hdr1");
is($hdr1->{'comp_suffix'}, '.TEENSY', "'comp_suffix' for hdr1");
is($hdr1->{'encrypt_suffix'}, 'E', "'encrypt_suffix' for hdr1");
is($hdr1->{'name'}, 'schlitz', "'name' for hdr1");
is($hdr1->{'disk'}, '/pbr', "'disk' for hdr1");
is($hdr1->{'program'}, 'GNUTAR', "'program' for hdr1");
is($hdr1->{'application'}, 'amnot', "'application' for hdr1");
is($hdr1->{'srvcompprog'}, 'scp', "'srvcompprog' for hdr1");
is($hdr1->{'srv_encrypt'}, 'se', "'srv_encrypt' for hdr1");
is($hdr1->{'recover_cmd'}, 'rec |', "'recover_cmd' for hdr1");
is($hdr1->{'uncompress_cmd'}, 'unc |', "'uncompress_cmd' for hdr1");
is($hdr1->{'decrypt_cmd'}, 'dec |', "'decrypt_cmd' for hdr1");
is($hdr1->{'srv_decrypt_opt'}, '-dos', "'srv_decrypt_opt' for hdr1");
is($hdr1->{'cont_filename'}, '/path/to/cont', "'cont_filename' for hdr1");
is($hdr1->{'dle_str'}, "D\nL\nE", "'dle_str' for hdr0");
is($hdr1->{'is_partial'}, 1, "'is_partial' for hdr1");
# no partnum for F_DUMPFILE
# no numparts for F_DUMPFILE
is($hdr1->{'blocksize'}, 0, "'blocksize' for hdr1 (not re-read; defaults to 0)");
is($hdr1->{'orig_size'}, 10240, "'orig_size' for hdr1");

is($hdr2->{'type'}, $Amanda::Header::F_SPLIT_DUMPFILE, "'type' for hdr2");
is($hdr2->{'datestamp'}, '20090102030405', "'datestamp' for hdr2");
is($hdr2->{'dumplevel'}, 6, "'dumplevel' for hdr2");
is($hdr2->{'compressed'}, 1, "'compressed' for hdr2");
is($hdr2->{'encrypted'}, 1, "'encrypted' for hdr2");
is($hdr2->{'comp_suffix'}, '.TEENSY', "'comp_suffix' for hdr2");
is($hdr2->{'encrypt_suffix'}, 'E', "'encrypt_suffix' for hdr2");
is($hdr2->{'name'}, 'schlitz', "'name' for hdr2");
is($hdr2->{'disk'}, '/pbr', "'disk' for hdr2");
is($hdr2->{'program'}, 'GNUTAR', "'program' for hdr2");
is($hdr2->{'application'}, 'amnot', "'application' for hdr2");
is($hdr2->{'clntcompprog'}, 'ccp', "'clntcompprog' for hdr2");
is($hdr2->{'clnt_encrypt'}, 'ce', "'clnt_encrypt' for hdr2");
is($hdr2->{'recover_cmd'}, 'rec |', "'recover_cmd' for hdr2");
is($hdr2->{'uncompress_cmd'}, 'unc |', "'uncompress_cmd' for hdr2");
is($hdr2->{'decrypt_cmd'}, 'dec |', "'decrypt_cmd' for hdr2");
is($hdr2->{'clnt_decrypt_opt'}, '-doc', "'clnt_decrypt_opt' for hdr2");
is($hdr2->{'cont_filename'}, '/path/to/cont', "'cont_filename' for hdr2");
is($hdr2->{'dle_str'}, "D\nL\nE", "'dle_str' for hdr0");
is($hdr2->{'is_partial'}, 1, "'is_partial' for hdr2");
is($hdr2->{'partnum'}, 13, "'partnum' for hdr2");
is($hdr2->{'totalparts'}, 14, "'totalparts' for hdr2");
is($hdr2->{'blocksize'}, 0, "'blocksize' for hdr2 (not re-read; defaults to 0)");
is($hdr2->{'orig_size'}, 10240, "'orig_size' for hdr2");

is($hdr3->{'type'}, $Amanda::Header::F_TAPESTART, "'type' for hdr3");
is($hdr3->{'datestamp'}, "20090102030405", "'datestamp' for F_TAPESTART");
is($hdr3->{'name'}, "TAPE17", "'name' for F_TAPESTART");

# test out the other methods

# debug_dump just shouldn't crash, please
$hdr1->debug_dump();
$hdr2->debug_dump();
$hdr3->debug_dump();

is($hdr1->summary(),
      "FILE: date 20090102030405 host schlitz disk /pbr lev 6 comp .TEENSY "
    . "program GNUTAR server_custom_compress scp server_encrypt se "
    . "server_decrypt_option -dos",
    "hdr1 summary");
is($hdr2->summary(),
      "split dumpfile: date 20090102030405 host schlitz disk /pbr part 13/14 lev 6 comp .TEENSY "
    . "program GNUTAR client_custom_compress ccp client_encrypt ce "
    . "client_decrypt_option -doc",
    "hdr2 summary");
is($hdr3->summary(),
    "start of tape: date 20090102030405 label TAPE17",
    "hdr3 summary");
