# Copyright (c) 2011 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::DB::Catalog2::log;

=head1 NAME

Amanda::DB::Catalog2::log - access to the Amanda catalog with log.

=head1 SYNOPSIS

This package implements the "log" catalog.  See C<amanda-catalog(7)>.

=cut

use strict;
use warnings;
use vars qw( @ISA );
@ISA = qw( Amanda::DB::Catalog2 );
#use base qw( Amanda::DB::Catalog2 );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Debug qw ( :logging );
use Amanda::Util qw( quote_string nicedate );
use Amanda::Tapelist;

sub new {
    my $class = shift;
    my $catalog_conf = shift;
    my %params = @_;
    my $plugin;
    my $properties;
    my $catalog_name;

    if (!$catalog_conf) {
	$catalog_name = getconf($CNF_CATALOG);
	if ($catalog_name) {
	    debug("catalog_name: $catalog_name");
	    $catalog_conf = lookup_catalog($catalog_name) if $catalog_name;
	}
    }

    if (!$catalog_conf) {
	$plugin = "log";
    } else {
	$plugin = Amanda::Config::catalog_getconf($catalog_conf,
						 $CATALOG_PLUGIN);
	$properties = Amanda::Config::catalog_getconf($catalog_conf,
						  $CATALOG_PROPERTY);
    }

    my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
    my ($tl, $message) = Amanda::Tapelist->new($tlf);
    if (defined $message) {
	return $message;
    }

    debug("using $plugin catalog");

    my $self = bless {
	config_name   => get_config_name(),
        tapelist      => $tl,
    }, $class;

    return $self;
}

sub DESTROY {
    my $self = shift;
    $self->quit();
}

sub quit {
    my $self = shift;

    delete $self->{'tapelist'};
}

sub get_version {
    my $self = shift;

    return $self->get_DB_VERSION();
}

sub add_simple_dump {
    my $self = shift;
    my $host_name = shift;
    my $disk_name = shift;
    my $device = shift || $disk_name;
    my $dump_timestamp = shift;
    my $level = shift;
    my $pool = shift;
    my $storage = shift;
    my $label = shift;
    my $filenum = shift;
    my $retention_days = shift;
    my $retention_full = shift;
    my $retention_recover = shift;

    die("add_simple_dump not defined");
}

sub add_image {
    my $self = shift;
    my $host_name = shift;
    my $disk_name = shift;
    my $device = shift || $disk_name;
    my $dump_timestamp = shift;
    my $level = shift;
    my $based_on_timestamp = shift;
    my $pid = shift;

    die("add_image not defined");
}

sub find_image {
    my $self = shift;
    my $host_name = shift;
    my $disk_name = shift;
    my $device = shift;
    my $dump_timestamp = shift;
    my $level = shift;

    die("find_image not defined");
}

sub get_image {
    my $self = shift;
    my $image_id = shift;

    die("get_image not defined");
}

sub get_copy {
    my $self = shift;
    my $copy_id = shift;

    die("get_copy not defined");
}

sub reset_volume {
    my $self = shift;
    my $pool_name = shift;
    my $label = shift;

    die("reset_volume not defined");
}

sub remove_volume {
    my $self = shift;
    my $pool_name = shift;
    my $label = shift;

    die("remove_volume not defined");
}

sub add_volume {
    my $self = shift;
    my $pool_name = shift;
    my $label = shift;
    my $write_timestamp = shift;
    my $storage_name = shift;
    my $meta_label = shift;
    my $barcode = shift;
    my $blocksize = shift || 32;
    my $reuse = shift || 0;
    my $retention_days = shift;
    my $retention_full = shift;
    my $retention_recover = shift;
    my $retention_tape = shift;

    $self->{'tapelist'}->reload(1);
    $self->{'tapelist'}->remove_tapelabel($label);
    # the label is not yet assigned a config
    $self->{'tapelist'}->add_tapelabel("0", $label, undef, 1, $meta_label, $barcode,
				$blocksize,
				$pool_name, $storage_name, Amanda::Config::get_config_name());
    $self->{'tapelist'}->write();

#    $self->user_msg(Amanda::Label::Message->new(
#				source_filename => __FILE__,
#				source_line => __LINE__,
#				code      => 1000022,
#				severity  => $Amanda::Message::SUCCESS));
    return;
}

sub find_meta {
    my $self = shift;
    my $meta = shift;

    die("find_meta not defined");
}

sub get_last_reusable_volume {
    my $self = shift;
    my $storage = shift;
    my $max_volume = shift || 1;

    die("get_last_reusable_volume not defined");
}

sub find_volume {
    my $self = shift;
    my $pool = shift;
    my $label = shift;
    my @volumes;

#print("find_volume $pool : $label\n");
    foreach my $tle (@{$self->{'tapelist'}->{'tles'}}) {
	next if defined $pool && $tle->{'pool'} ne $pool;
	next if defined $label && $tle->{'label'} ne $label;
	return Amanda::DB::Catalog2::log::volume->new($self,
				pool    => $tle->{'pool'},
				label   => $tle->{'label'},
				reuse   => $tle->{'reuse'},
				storage => $tle->{'storage'},
				config  => $tle->{'config'},
				write_timestamp    => $tle->{'datestamp'},
				retention_days    => -1,
				retention_full    => -1,
				retention_recover    => -1,
				retention_tape    => -1,
				blocksize    => $tle->{'blocksize'},
				barcode    => $tle->{'barcode'});
    }

#print("find_volume return undef\n");
    return undef;
}

sub find_volume_by_barcode {
    my $self = shift;
    my $barcode = shift;

    die("find_volume_by_barcode not defined");
}

sub find_volume_all {
    my $self = shift;
    my $pool = shift;
    my $label = shift;
    my @volumes;
#print("find_volume_all $pool : $label\n");

    my $tle = $self->{'tapelist'}->lookup_tape_by_pool_label($pool, $label);
    return undef if !$tle;

    push @volumes, Amanda::DB::Catalog2::log::volume->new($self,
				pool =>    $tle->{'pool'},
				label =>    $tle->{'label'},
				storage => $tle->{'storage'},
				meta    => $tle->{'meta'},
				config  => $tle->{'config'});

    return @volumes;
}

sub find_volumes {
    my $self = shift;
    my %params = @_;
    my @volumes;
    my $count = 0;

    foreach my $tle (@{$self->{'tapelist'}->{'tles'}}) {
	next if defined $params{pool} && $tle->{'pool'} ne $params{pool};
	next if defined $params{label} && $tle->{'label'} ne $params{label};
	next if defined $params{reuse} && $tle->{'reuse'} != $params{reuse};
	next if defined $params{storage} && ($tle->{'storage'} ne $params{storage} && $tle->{'storage'} ne '' && defined $tle->{'storage'});
	next if defined $params{storage_only} && $tle->{'storage'} ne $params{storage_only};
	next if defined $params{config} && ($tle->{'config'} ne $params{config} && $tle->{'config'} ne '' && defined $tle->{'config'});
	next if defined $params{only_config} && $tle->{'config'} ne $params{only_config};
	next if defined $params{meta} && $tle->{'meta'} ne $params{meta};
	next if defined $params{barcode} && $tle->{'barcode'} ne $params{barcode};
	next if defined $params{write_timestamp} && $tle->{'datestamp'} != 0;
	next if defined $params{write_timestamp_set} && $tle->{'datestamp'} == 0;
	next if defined $params{'max_volume'} && $count >= $params{'max_volume'};
#JLM	die ("no params{storages} in find_volumes") if defined $params{storages};
#JLM	die ("no params{order_write_timestamp} in find_volumes") if defined $params{order_write_timestamp};
#JLM	die ("no params{retention} in find_volumes") if defined $params{retention};
#JLM	die ("no params{retention_tape} in find_volumes") if defined $params{retention_tape};
#JLM	die ("no params{retention_name} in find_volumes") if defined $params{retention_name};

	$count++;
	my $volume;
	if ($params{no_bless}) {
	    $volume = {
				write_timestamp =>    $tle->{'datestamp'},
				reuse =>    $tle->{'reuse'},
				pool =>    $tle->{'pool'},
				label =>    $tle->{'label'},
				storage => $tle->{'storage'},
				barcode => $tle->{'barcode'},
				meta    => $tle->{'meta'},
				config  => $tle->{'config'}};
	} else {
	    $volume = Amanda::DB::Catalog2::log::volume->new($self,
				write_timestamp =>    $tle->{'datestamp'},
				reuse =>    $tle->{'reuse'},
				pool =>    $tle->{'pool'},
				label =>    $tle->{'label'},
				storage => $tle->{'storage'},
				barcode => $tle->{'barcode'},
				meta    => $tle->{'meta'},
				config  => $tle->{'config'});
	}
	push @volumes, $volume;
    }

    return @volumes;
}

sub set_no_reuse {
    my $self = shift;
    my $pool = shift;
    my $label = shift;

    die("set_no_reuse not defined");
}

sub remove_volume {
    my $self = shift;
    my $pool = shift;
    my $label = shift;

    die("remove_volume not defined");
}

sub add_flush_command {
    my $self = shift;
    my %params = @_;

    die("add_flush_command not defined");
}

sub get_cmdflush_ids_for_holding {
    my $self = shift;
    my $holding_file = shift;

    die("get_cmdflush_ids_for_holding not defined");
}

sub get_command_ids_for_holding {
    my $self = shift;
    my $holding_file = shift;

    die("get_command_ids_for_holding not defined");
}

sub merge {
    my $self = shift;

    die("merge not defined");
}

sub validate {
    my $self = shift;

    die("validate not defined");
}

sub remove_working_cmd {
    my $self = shift;
    my $pid = shift;

    die("remove_working_cmd not defined");
}

sub remove_cmd {
    my $self = shift;
    my $pid = shift;

    die("remove_cmd not defined");
}

sub add_flush_cmd {
    my $self = shift;
    my %params = @_;

    die("add_flush_cmd not defined");
}

sub get_flush_command {
    my $self = shift;

    die("get_flush_command not defined");
}

sub get_command_from_id {
    my $self = shift;
    my $id = shift;

    die("get_command_from_id not defined");
}

sub clean {
    my $self = shift;

    die("clean not defined");
}

sub get_latest_write_timestamp {
    my $self = shift;

    return Amanda::DB::Catalog::get_latest_write_timestamp(@_);
}

sub get_latest_run_timestamp {
    my $self = shift;

    return Amanda::DB::Catalog::get_latest_write_timestamp();
}

sub get_dumps {
    my $self = shift;
    my %params = @_;

    return Amanda::DB::Catalog::get_dumps(%params);
}

sub get_parts {
    my $self = shift;
    my $dumps = shift;
    my %params = @_;

    my @parts;

    if (ref($dumps) ne 'ARRAY') {
        $dumps = [ $dumps ];
    }

    for my $dump (@$dumps) {
        # find matching part
        my @dump_parts;
	for my $part (@{$dump->{parts}}) {
	    next if !defined $part;
	    if ($params{'holding'}) {
		next if !exists $part->{'holding_file'};
	    }
	    if ($params{'label'}) {
		next if $part->{'label'} ne $params{'label'};
	    }
	    if ($params{'labels'}) {
		my $matched = 0;
		foreach my $label (@{$params{'labels'}}) {
		    if (exists $part->{'label'} and $label eq $part->{'label'}) {
			$matched = 1;
			last;
		    }
		}
		next if $matched == 0;
	    }

	    $dump_parts[$part->{'partnum'}] = $part;
	    #push @dump_parts, $part;
	    push @parts, $part;
	}
	$dump->{'parts'} = \@dump_parts;
    }

    return @parts;
}

sub sort_dumps {
    my $self = shift;

    return Amanda::DB::Catalog::sort_dumps(@_);
}

sub sort_parts {
    my $self = shift;

    return Amanda::DB::Catalog::sort_parts(@_);
}

sub get_write_timestamps {
    my $self = shift;

    return Amanda::DB::Catalog::get_write_timestamps();
}

sub get_run_type {
    my $self = shift;
    my $write_timestamp = shift;

    return Amanda::DB::Catalog::get_run_type($write_timestamp);
}

sub print_catalog {
    my $self = shift;
    my $dumpspecs = shift;
    my %params = @_;
    my $nb_dumpspec = @$dumpspecs;

    my @dumps;
    my @parts;
    if (@$dumpspecs == 0) {
        @dumps = $self->get_dumps();
    } else {
        @dumps = $self->get_dumps(dumpspecs => [ @$dumpspecs ]);
    }

    if (!$params{'parts'}) {
	for my $dump (@dumps) {
            print $self->{'config_name'} . " " .
		  nicedate($dump->{'dump_timestamp'}) . " " .
                  quote_string($dump->{'hostname'}) . " " .
                  quote_string($dump->{'diskname'}) . " " .
                  quote_string($dump->{'storage'}) . " " .
                  $dump->{'level'} . " " .
                  $dump->{'status'} . " " .
                  $dump->{'status'} . " " .
                  "0 0\n";
	}
    } else {
        if (@$dumpspecs == 0) {
            @parts = $self->get_parts(\@dumps);
        } else {
            @parts = $self->get_parts(\@dumps, dumpspecs => [ @$dumpspecs ]);
        }
	for my $part (@parts) {
            my $label = $part->{'label'};
            $label = $part->{'holding_file'} if !$label;
            my $filenum = $part->{'filenum'};
            $filenum = "0" if !$filenum;
	    my $status = $part->{'dump'}->{'status'};
	    $status = "" if $part->{'status'} eq "OK" and
			    $part->{'dump'}->{'status'} eq "OK";
            print $self->{'config_name'} . " " .
		  nicedate($part->{'dump'}->{'dump_timestamp'}) . " " .
                  quote_string($part->{'dump'}->{'hostname'}) . " " .
                  quote_string($part->{'dump'}->{'diskname'}) . " " .
                  $part->{'dump'}->{'level'} . " " .
                  $part->{'dump'}->{'storage'} . " " .
                  $part->{'pool'} . " " .
                  $label . " " .
                  $part->{'dump'}->{'status'} . " " .
                  $part->{'dump'}->{'status'} . " " .
                  $part->{'status'} . " " .
                  $filenum . " " .
                  $part->{'dump'}->{'nparts'} . " " .
                  $part->{'partnum'} . "\n";
	}
    }
}

sub volume_assign {
    my $self = shift;
    my $force = shift;
    my $pool = shift;
    my $label = shift;
    my $new_pool = shift;
    my $barcode = shift;
    my $storage = shift;
    my $meta = shift;
    my $reuse = shift;
    my @result_messages;
    my $changed = 0;
    my $changed1 = 0;
    my $error = 0;


    $self->{'tapelist'}->reload(1);
    my $tle = $self->{'tapelist'}->lookup_tape_by_pool_label($pool, $label);
    if (!defined $tle) {
	return Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000035,
			severity => $Amanda::Message::ERROR,
			label  => $label,
			pool   => $pool,
			catalog_name => $self->{'catalog_name'});
    }

    if ($tle->{'config'} &&
	$tle->{'config'} ne Amanda::Config::get_config_name()) {
	push @result_messages, Amanda::Label::Message->new(
			source_filename => __FILE__,
			source_line => __LINE__,
			code   => 1000000,
			severity => $Amanda::Message::ERROR,
			label  => $label,
			pool   => $pool,
			config => $tle->{'config'},
			catalog_name => $self->{'catalog_name'});
	return @result_messages;
    }

    if (!$tle->{'config'} && $tle->{'datestamp'} ne "0") {
	$tle->{'config'} = Amanda::Config::get_config_name();
	$changed1 = 1;
    }

    if (defined $reuse) {
	if ($reuse == $tle->{'reuse'}) {
	    # message same reuse
	    push @result_messages, Amanda::Label::Message->new(
                        source_filename => __FILE__,
                        source_line => __LINE__,
                        code   => $reuse?1000046:1000048,
                        severity => $Amanda::Message::INFO,
                        label  => $label,
                        pool   => $pool,
                        catalog_name => $self->{'catalog_name'});
	} else {
	    $tle->{'reuse'} = $reuse;
	    $changed1 = 1;
	}
    }

    if (defined $barcode) {
	if ($barcode eq $tle->{'barcode'}) {
	    # message same barcode
	} elsif (!$force && $tle->{'barcode'}) {
	    push @result_messages, Amanda::Label::Message->new(
                        source_filename => __FILE__,
                        source_line => __LINE__,
                        code   => 1000002,
                        severity => $Amanda::Message::ERROR,
                        label  => $label,
                        pool   => $pool,
                        barcode => $tle->{'barcode'},
                        catalog_name => $self->{'catalog_name'});
	    $error = 1;
	} elsif (!defined $tle->{'barcode'} or
		 $tle->{'barcode'} ne $barcode) {
	    $tle->{'barcode'} = $barcode;
	    $tle->{'barcode'} = undef if $barcode eq '';
	    $changed1 = 1;
	}
    }

    if (defined $storage) {
	if ($storage eq '') {
	    push @result_messages, Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000059,
					severity => $Amanda::Message::ERROR,
					label  => $tle->{'label'},
					pool   => $pool,
					catalog_name => $self->{'catalog_name'});
	    $error = 1;
	} elsif (defined($tle->{'storage'}) &&
                            $storage ne $tle->{'storage'} &&
                            !$force) {
	    push @result_messages, Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code    => 1000004,
					severity => $Amanda::Message::ERROR,
					label   => $tle->{'label'},
					pool   => $pool,
					storage => $tle->{'storage'},
					catalog_name => $self->{'catalog_name'});
	    $error = 1;
	} elsif ($tle->{'datestamp'} eq "0") {
	    push @result_messages, Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000005,
					severity => $Amanda::Message::ERROR,
					label  => $tle->{'label'},
					pool   => $pool,
					catalog_name => $self->{'catalog_name'});
	    $error = 1;
	} elsif (!defined $tle->{'storage'} or
		 $tle->{'storage'} ne $storage) {
	    $tle->{'storage'} = $storage;
	    $tle->{'storage'} = undef if $storage eq '';
	    $changed1 = 1;
	}
    }

    if (defined $new_pool) {
	if ($new_pool eq '') {
	    push @result_messages, Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000058,
					severity => $Amanda::Message::ERROR,
					label  => $tle->{'label'},
					catalog_name => $self->{'catalog_name'});
	    $error = 1;
	} elsif (defined($tle->{'pool'}) &&
		 $new_pool ne $tle->{'pool'} &&
		 !$force) {
	    push @result_messages, Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000003,
					severity => $Amanda::Message::ERROR,
					label  => $tle->{'label'},
					pool   => $tle->{'pool'},
					catalog_name => $self->{'catalog_name'});
	    $error = 1;
	} elsif (!defined $tle->{'pool'} or
		 $tle->{'pool'} ne $new_pool) {
	    $tle->{'pool'} = $new_pool;
	    $tle->{'pool'} = undef if $new_pool eq '';
	    $changed1 = 1;
	}
    }

    if (defined $meta) {
	if (defined($tle->{'meta'}) &&
	    $meta ne $tle->{'meta'} &&
	    !$force) {
	    push @result_messages, Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000001,
					severity => $Amanda::Message::ERROR,
					label  => $tle->{'label'},
					pool   => $tle->{'pool'},
					meta => $tle->{'meta'},
					catalog_name => $self->{'catalog_name'});
	    $error = 1;
	} elsif (!defined $tle->{'meta'} or
		 $tle->{'meta'} ne $meta) {
	    $tle->{'meta'} = $meta;
	    $tle->{'meta'} = undef if $meta eq '';
	    $changed1 = 1;
	}
    }

    if ($changed1 && !$error) {
	push @result_messages, Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000006,
					severity => $Amanda::Message::SUCCESS,
					label  => $tle->{'label'},
					pool   => $tle->{'pool'},
					catalog_name => $self->{'catalog_name'});
	$changed++;
    }
    if ($error == 1) {
    } elsif ($changed) {
	$self->{'tapelist'}->write();
    } else {
	$self->{'tapelist'}->unlock();
	push @result_messages, Amanda::Label::Message->new(
					source_filename => __FILE__,
					source_line => __LINE__,
					code   => 1000007,
					severity => $Amanda::Message::INFO,
					label  => $tle->{'label'},
					pool   => $tle->{'pool'},
					catalog_name => $self->{'catalog_name'});
    }
    return \@result_messages;
}

sub list_retention {
    my $self     = shift;

    return Amanda::Tapelist::list_retention();
}

sub list_no_retention {
    my $self     = shift;

    return Amanda::Tapelist::list_no_retention();
}

package Amanda::DB::Catalog2::log::volume;

sub new {
    my $class     = shift;
    my $catalog   = shift;

    my $self = bless {
	catalog   => $catalog,
	@_,
    }, $class;

    return $self;
}

sub set {
}

sub unset {
}

sub set_write_timestamp {
}

package Amanda::DB::Catalog2::log::image;

sub new {
    my $class     = shift;
    my $catalog   = shift;
    my $image_id  = shift;

    my $self = bless {
	catalog  => $catalog,
	image_id => $image_id,
    }, $class;

    return $self;
}

sub add_copy {
    my $self = shift;
    my $write_timestamp = shift;

    my $catalog = $self->{'catalog'};
    return Amanda::DB::Catalog2::log::copy->new($catalog, 0);
}

sub finish_image {
    my $self           = shift;
    my $orig_kb        = shift;
    my $kb             = shift;
    my $volume_bytes   = shift;
    my $dump_status    = shift;
    my $nb_files       = shift;
    my $nb_directories = shift;
}


package Amanda::DB::Catalog2::log::copy;

sub new {
    my $class   = shift;
    my $catalog = shift;
    my $copy_id = shift;

    my $self = bless {
	catalog => $catalog,
	copy_id => $copy_id,
    }, $class;

    return $self;
}

sub add_part {
    my $self        = shift;
    my $volume      = shift;
    my $part_offset = shift;
    my $part_size   = shift;
    my $filenum     = shift;
    my $part_num    = shift;
    my $part_status = shift;
}

sub finish_copy {
    my $self        = shift;
    my $nb_part     = shift;
    my $copy_status = shift;
}

1;
