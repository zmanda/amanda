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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Installcheck::Config;
use Installcheck;
use Amanda::Paths;
use Amanda::Constants;
use File::Path;
use Carp;

=head1 NAME

Installcheck::Config - set up amanda configurations for installcheck testing

=head1 SYNOPSIS

  use Installcheck::Config;

  my $testconf = Installcheck::Config->new();
  $testconf->add_param("runtapes", "5");
  $testconf->add_tapetype("DUCKTAPE", [
    length => "10G", filemark => "4096k",
  ]);
  # ...
  $testconf->write();

The resulting configuration is always named "TESTCONF".  The basic
configuration contains only a few parameters that are necessary
just to run Amanda applications in the test environment.  It also
contains a tapetype, C<TEST-TAPE>.  To change tapetype parameters,
call C<< $cf->add_tapetype >> with a new definition of C<TEST-TAPE>.

Note that it's quite possible to produce an invalid configuration with this
package (and, in fact, some of the tests do just that).

=head1 WARNING

Using this module I<will> destroy any existing configuration named
TESTDIR.  I<Please> do not use this on a production machine!

=head1 FUNCTIONS

=over

=item C<new()>

Create a new configuration object

=cut

sub new {
    my $class = shift;

    # An instance is a blessed hash containing parameters.  Start with
    # some defaults to make sure things run.
    my $infofile = "$CONFIG_DIR/TESTCONF/curinfo";
    my $logdir = "$CONFIG_DIR/TESTCONF/log";
    my $indexdir = "$CONFIG_DIR/TESTCONF/index";
    my $org = "DailySet1";

    my $self = {
	'infofile' => $infofile,
	'logdir' => $logdir,
	'indexdir' => $indexdir,

	# Global params are stored as an arrayref, so that the same declaration
	# can appear multiple times
	'params' => [
	    'dumpuser' => '"' . (getpwuid($<))[0] . '"', # current username

	    # These dirs are under CONFIG_DIR just for ease of destruction.
	    # This is not a recommended layout!
	    'infofile' => "\"$infofile\"",
	    'logdir' => "\"$logdir\"",
	    'indexdir' => "\"$indexdir\"",
	    'org' => "\"$org\"",

	    # (this is actually added while writing the config file, if not
	    # overridden by the caller)
	    # 'tapetype' => '"TEST-TAPE"',
	],

	# global client config
	'client_params' => [
	    'amandates' => "\"$Installcheck::TMP/TESTCONF/amandates\"",
	    'gnutar_list_dir' => "\"$Installcheck::TMP/TESTCONF/gnutar_listdir\"",
	],

	# config-specific client config
	'client_config_params' => [
	],

	# Subsections are stored as a hashref of arrayrefs, keyed by
	# subsection name

	'tapetypes' => [ ],
	'dumptypes' => [ ],
	'interfaces' => [ ],
	'holdingdisks' => [ ],
	'application' => [ ],
	'script' => [ ],
	'devices' => [ ],
	'changers' => [ ],
	'text' => '',

	'dles' => [ ],
    };
    bless($self, $class);

    $self->add_tapetype('TEST-TAPE', [
	'length' => '50 mbytes',
	'filemark' => '4 kbytes'
    ]);
    return $self;
}

=item C<add_param($param, $value)>

Add the given parameter to the configuration file.  Note that strings which
should be quoted in the configuration file itself must be double-quoted here,
e.g.,

  $testconf->add_param('org' => '"MyOrganization"');

=cut

sub add_param {
    my $self = shift;
    my ($param, $value) = @_;

    push @{$self->{'params'}}, $param, $value;
}

=item C<add_client_param($param, $value)>, C<add_client_config_param($param, $value)>

Add the given parameter to the client configuration file, as C<add_param> does
for the server configuration file.  C<add_client_param> addresses the global
client configuration file, while C<add_client_config_param> inserts parmeters
into C<TESTCONF/amanda-client.conf>.

  $testconf->add_client_param('auth' => '"krb2"');
  $testconf->add_client_config_param('client_username' => '"freddy"');

=cut

sub add_client_param {
    my $self = shift;
    my ($param, $value) = @_;

    push @{$self->{'client_params'}}, $param, $value;
}

sub add_client_config_param {
    my $self = shift;
    my ($param, $value) = @_;

    push @{$self->{'client_config_params'}}, $param, $value;
}

=item C<remove_param($param)>

Remove the given parameter from the config file.

=cut

sub remove_param {
    my $self = shift;
    my ($param) = @_;

    my @new_params;

    while (@{$self->{'params'}}) {
	my ($p, $v) = (shift @{$self->{'params'}}, shift @{$self->{'params'}});
	next if $p eq $param;
	push @new_params, $p, $v;
    }

    $self->{'params'} = \@new_params;
}

=item C<add_tapetype($name, $values_arrayref)>
=item C<add_dumptype($name, $values_arrayref)>
=item C<add_holdingdisk($name, $values_arrayref)>
=item C<add_holdingdisk_def($name, $values_arrayref)>
=item C<add_interface($name, $values_arrayref)>
=item C<add_application($name, $values_arrayref)>
=item C<add_script($name, $values_arrayref)>
=item C<add_device($name, $values_arrayref)>
=item C<add_changer($name, $values_arrayref)>
=item C<add_interactivity($name, $values_arrayref)>
=item C<add_taperscan($name, $values_arrayref)>

Add the given subsection to the configuration file, including all values in the
arrayref.  The values should be specified as alternating key/value pairs.
Since holdingdisk definitions usually don't have a "define" keyword,
C<add_holdingdisk> does not add one, but C<add_holdingdisk_def> does.

=cut

sub _add_subsec {
    my $self = shift;
    my ($subsec, $name, $use_define, $values) = @_;

    # first delete any existing subsections with that name
    @{$self->{$subsec}} = grep { $_->[0] ne $name } @{$self->{$subsec}};
    
    # and now push the new subsection definition on the end
    push @{$self->{$subsec}}, [$name, $use_define, $values];
}

sub add_tapetype {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("tapetypes", $name, 1, $values);
}

sub add_dumptype {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("dumptypes", $name, 1, $values);
}

# by default, holdingdisks don't have the "define" keyword
sub add_holdingdisk {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("holdingdisks", $name, 0, $values);
}

# add a holdingdisk definition only (use "define" keyword)
sub add_holdingdisk_def {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("holdingdisks", $name, 1, $values);
}

sub add_interface {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("interfaces", $name, 1, $values);
}

sub add_application {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("application", $name, 1, $values);
}

sub add_script {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("script", $name, 1, $values);
}

sub add_device {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("devices", $name, 1, $values);
}

sub add_changer {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("changers", $name, 1, $values);
}

sub add_interactivity {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("interactivities", $name, 1, $values);
}

sub add_taperscan {
    my $self = shift;
    my ($name, $values) = @_;
    $self->_add_subsec("taperscans", $name, 1, $values);
}

=item C<add_text($text)>

Add arbitrary text to the config file.

=cut

sub add_text {
    my $self = shift;
    my ($text) = @_;
    $self->{'text'} .= $text;
}

=item C<add_dle($line)>

Add a disklist entry; C<$line> is inserted verbatim into the disklist.

=cut

sub add_dle {
    my $self = shift;
    my ($line) = @_;
    push @{$self->{'dles'}}, $line;
}

=item C<write()>

Write out the accumulated configuration file, along with any other
files necessary to run Amanda.

=cut

sub write {
    my $self = shift;

    cleanup();

    my $testconf_dir = "$CONFIG_DIR/TESTCONF";
    mkpath($testconf_dir);

    # set up curinfo dir, etc.
    mkpath($self->{'infofile'}) or die("Could not create infofile directory");
    mkpath($self->{'logdir'}) or die("Could not create logdir directory");
    mkpath($self->{'indexdir'}) or die("Could not create indexdir directory");
    my $amandates = $Installcheck::TMP . "/TESTCONF/amandates";
    my $gnutar_listdir = $Installcheck::TMP . "/TESTCONF/gnutar_listdir";
    if (! -d $gnutar_listdir) {
	mkpath($gnutar_listdir)
	    or die("Could not create '$gnutar_listdir'");
    }

    $self->_write_tapelist("$testconf_dir/tapelist");
    $self->_write_disklist("$testconf_dir/disklist");
    $self->_write_amanda_conf("$testconf_dir/amanda.conf");
    $self->_write_amandates($amandates);
    $self->_write_amanda_client_conf("$CONFIG_DIR/amanda-client.conf");
    $self->_write_amanda_client_config_conf("$testconf_dir/amanda-client.conf");
}

sub _write_tapelist {
    my $self = shift;
    my ($filename) = @_;

    # create an empty tapelist
    open(my $tapelist, ">", $filename);
    close($tapelist);
}

sub _write_disklist {
    my $self = shift;
    my ($filename) = @_;

    # don't bother writing a disklist if there are no dle's
    return unless $self->{'dles'};

    open(my $disklist, ">", $filename);

    for my $dle_line (@{$self->{'dles'}}) {
	print $disklist "$dle_line\n";
    }

    close($disklist);
}

sub _write_amanda_conf {
    my $self = shift;
    my ($filename) = @_;

    open my $amanda_conf, ">", $filename
	or croak("Could not open '$filename'");

    # write key/value pairs
    my @params = @{$self->{'params'}};
    my $saw_tapetype = 0;
    my $taperscan;
    while (@params) {
	$param = shift @params;
	$value = shift @params;
	if ($param eq 'taperscan') {
	    $taperscan = $value;
	    next;
	}
	print $amanda_conf "$param $value\n";
	$saw_tapetype = 1 if ($param eq "tapetype");
    }

    # tapetype is special-cased: if the user has not specified a tapetype, use "TEST-TAPE".
    if (!$saw_tapetype) {
	print $amanda_conf "tapetype \"TEST-TAPE\"\n";
    }

    # write out subsections
    $self->_write_amanda_conf_subsection($amanda_conf, "tapetype", $self->{"tapetypes"});
    $self->_write_amanda_conf_subsection($amanda_conf, "application", $self->{"application"});
    $self->_write_amanda_conf_subsection($amanda_conf, "script", $self->{"script"});
    $self->_write_amanda_conf_subsection($amanda_conf, "dumptype", $self->{"dumptypes"});
    $self->_write_amanda_conf_subsection($amanda_conf, "interface", $self->{"interfaces"});
    $self->_write_amanda_conf_subsection($amanda_conf, "holdingdisk", $self->{"holdingdisks"});
    $self->_write_amanda_conf_subsection($amanda_conf, "device", $self->{"devices"});
    $self->_write_amanda_conf_subsection($amanda_conf, "changer", $self->{"changers"});
    $self->_write_amanda_conf_subsection($amanda_conf, "interactivity", $self->{"interactivities"});
    $self->_write_amanda_conf_subsection($amanda_conf, "taperscan", $self->{"taperscans"});
    print $amanda_conf "\n", $self->{'text'}, "\n";
    print $amanda_conf "taperscan $taperscan\n" if $taperscan;

    close($amanda_conf);
}

sub _write_amanda_conf_subsection {
    my $self = shift;
    my ($amanda_conf, $subsec_type, $subsec_ref) = @_;

    for my $subsec_info (@$subsec_ref) {
	my ($subsec_name, $use_define, $values) = @$subsec_info;
	
	my $define = $use_define? "define " : "";
	print $amanda_conf "\n$define$subsec_type $subsec_name {\n";

	my @values = @$values; # make a copy
	while (@values) {
	    $param = shift @values;
	    $value = shift @values;
	    if ($param eq "inherit") {
		print $amanda_conf "$value\n";
	    } else {
	        print $amanda_conf "$param $value\n";
	    }
	}
	print $amanda_conf "}\n";
    }
}

sub _write_amandates {
    my $self = shift;
    my ($filename) = @_;

    # make sure the containing directory exists
    mkpath($filename =~ /(^.*)\/amandates/);

    # truncate the file to eliminate any interference from previous runs
    open(my $amandates, ">", $filename) or die("Could not write to '$filename'");
    close($amandates);
}

sub _write_amanda_client_conf {
    my $self = shift;
    my ($filename, $amandates, $gnutar_listdir) = @_;

    # just an empty file for now
    open(my $amanda_client_conf, ">", $filename) 
	or croak("Could not write to '$filename'");

    # write key/value pairs
    my @params = @{$self->{'client_params'}};
    while (@params) {
	$param = shift @params;
	$value = shift @params;
	print $amanda_client_conf "$param $value\n";
    }

    close($amanda_client_conf);
}

sub _write_amanda_client_config_conf {
    my $self = shift;
    my ($filename, $amandates, $gnutar_listdir) = @_;

    # just an empty file for now
    open(my $amanda_client_conf, ">", $filename) 
	or croak("Could not write to '$filename'");

    # write key/value pairs
    my @params = @{$self->{'client_config_params'}};
    while (@params) {
	$param = shift @params;
	$value = shift @params;
	print $amanda_client_conf "$param $value\n";
    }

    close($amanda_client_conf);
}

=item C<cleanup()> (callable as a package method too)

Clean up by deleting the configuration directory.

=cut

sub cleanup {
    my $testconf_dir = "$CONFIG_DIR/TESTCONF";
    if (-e $testconf_dir) {
	rmtree($testconf_dir) or die("Could not remove '$testconf_dir'");
    }
}

1;
