# Copyright (c) 2013 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Rest::Storages::Labels;
use strict;
use warnings;

use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Device qw( :constants );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Header;
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Label;
use Amanda::Util qw( match_datestamp );
use Amanda::Rest::Configs;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Storages::Labels -- Rest interface to manage label

=head1 INTERFACE

=over

=item List labels of a storage

You can use the /amanda/v1.0/configs/:CONF/labels endpoint and filter with the storage.

 request:
  GET /amanda/v1.0/configs/:CONF/storages/:STORAGE/labels
  You can filter the labels listed with the following query arguments:
            config=CONF
            datestamp=datastamp_range
            storage=STORAGE
            meta=META
            pool=POOL
            reuse=0|1

 reply:
  HTTP status: 200 OK
  [
     {
        "code" : "1600001",
        "message" : "List of labels",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Storages/Labels.pm",
        "source_line" : "245",
        "tles" : [
           {
              "barcode" : null,
              "blocksize" : "32",
              "comment" : null,
              "config" : "test",
              "datestamp" : "20131118134146",
              "label" : "test-ORG-AA-vtapes-002",
              "meta" : "test-ORG-AA",
              "pool" : "my_vtapes",
              "position" : 45,
              "reuse" : "1",
              "storage" : "my_vtapes"
           }
	   ...
        ]
     }
  ]

=item List one label

 request:
  GET /amanda/v1.0/configs/:CONF/storages/:STORAGE/labels/:LABEL

 reply:
  HTTP status: 200 OK
  [
     {
        "code" : "1600001",
        "message" : "List of labels",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Rest/Storages/Labels.pm",
        "source_line" : "245",
        "tles" : [
           {
              "barcode" : null,
              "blocksize" : "32",
              "comment" : null,
              "config" : "test",
              "datestamp" : "20131118134146",
              "label" : "test-ORG-AA-vtapes-002",
              "meta" : "test-ORG-AA",
              "pool" : "my_vtapes",
              "position" : 45,
              "reuse" : "1",
              "storage" : "my_vtapes"
           }
        ]
     }
  ]

=item Modify label setting

 request:
  POST /amanda/v1.0/configs/:CONF/storages/:STORAGE/labels/:LABEL
    query arguments:
        force=0|1
        config=CONFIG
        storage=STORAGE
        meta=META
        barcode=BARCODE
        comment=COMMENT
        reuse=0|1

 reply:
  HTTP status: 200 OK
  [
     {
        "code" : "1000045",
        "label" : "test-ORG-AC-vtapes2-001",
        "message" : "marking tape 'test-ORG-AC-vtapes2-001' as reusable.",
        "severity" : "16",
        "source_filename" : "/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
        "source_line" : "1315"
     },
     {
        "code" : "1000006",
        "label" : "test-ORG-AC-vtapes2-001",
        "message" : "Setting test-ORG-AC-vtapes2-001",
        "severity" : "16",
        "source_filename" : "/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
        "source_line" : "408"
     }
  ]

=item Label a volume

 request:
  POST /amanda/v1.0/configs/:CONF/storages/:STORAGE/labels

 reply:
  HTTP status: 200 OK
  [
     {
        "code" : "1000008",
        "message" : "Reading label...",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Label.pm",
        "source_line" : "513"
     },
     {
        "code" : "1000009",
        "message" : "Found an empty tape.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Label.pm",
        "source_line" : "574"
     },
     {
        "code" : "1000020",
        "label" : "test-ORG-AA-vtapes-002",
        "message" : "Writing label 'test-ORG-AA-vtapes-002'...",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Label.pm",
        "source_line" : "707"
     },
     {
        "code" : "1000021",
        "message" : "Checking label...",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Label.pm",
        "source_line" : "727"
     },
     {
        "code" : "1000022",
        "message" : "Success!",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Label.pm",
        "source_line" : "769"
     }
  ]

=item Remove a label

 request:
  DELETE /amanda/v1.0/configs/:CONF/storages/:STORAGE/labels
  DELETE /amanda/v1.0/configs/:CONF/storages/:STORAGE/labels/:LABEL
    query arguments:
        remove_no_retention
        labels=LABEL1,LABEL2
        cleanup
        dry_run
        erase
        external_copy
        keep_label

 reply:
  HTTP status: 200 OK
  [
    {
        "code" : "1000052",
        "label" : "test-ORG-AA-vtapes-002",
        "message" : "Removed label 'test-ORG-AA-vtapes-002 from tapelist file.",
        "severity" : "16",
        "source_filename" : "/usr/lib/amanda/perl/Amanda/Label.pm",
        "source_line" : "1070",
        "tapelist_filename" : "/usr/amanda/test/tapelist"
     }
  ]

=back

=cut

sub init {
    my %params = @_;

    my $filename = config_dir_relative(getconf($CNF_TAPELIST));

    my ($tl, $message) = Amanda::Tapelist->new($filename);
    if (defined $message) {
	Dancer::Status(404);
	return $message;
    } elsif (!defined $tl) {
	Dancer::Status(404);
	return Amanda::Tapelist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code => 1600000,
				severity => $Amanda::Message::ERROR,
				tapefile => $filename);
    }
    return $tl;
}

sub label {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Storages::Labels");
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return @result_messages if @result_messages;

    my $tl = Amanda::Rest::Labels::init();
    if ($tl->isa("Amanda::Message")) {
	push @result_messages, $tl;
	return \@result_messages;
    }

    my @tles = grep {$_->{'label'} eq $params{'LABEL'}} @{$tl->{'tles'}};
    @tles = grep {$_->{'storage'} eq $params{'STORAGE'}} @tles if $params{'storage'};
    @tles = grep {$_->{'config'}  eq $params{'config'}}  @tles if $params{'config'};
    @tles = grep {$_->{'storage'} eq $params{'storage'}} @tles if $params{'storage'};
    @tles = grep {$_->{'pool'}    eq $params{'pool'}}    @tles if $params{'pool'};
    @tles = grep {$_->{'meta'}    eq $params{'meta'}}    @tles if $params{'meta'};
    @tles = grep {$_->{'reuse'}   eq $params{'reuse'}}   @tles if $params{'reuse'};
    push @result_messages, Amanda::Tapelist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code => 1600001,
				severity => $Amanda::Message::SUCCESS,
				tles => \@tles);
    return \@result_messages;
}

sub erase {
    my %params = @_;
    Amanda::Util::set_pname("Amanda::Rest::Storages::Labels");
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
	my $msg = shift;
	push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;

	my $steps = define_steps
	    cb_ref => \$finished_cb;

	step start => sub {

	    # amadmin may later try to load this and will die if it has errors
	    # load it now to catch the problem sooner (before we might erase data)
	    my $diskfile = config_dir_relative(getconf($CNF_DISKFILE));
	    my $cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
	    if ($cfgerr_level >= $CFGERR_ERRORS) {
		push @result_messages, Amanda::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code		=> 1,
				severity => $Amanda::Message::ERROR,
				message => "Errors processing disklist");
		$steps->{'done'}->();
	    }

            my $tl = Amanda::Rest::Labels::init();
	    if ($tl->isa("Amanda::Message")) {
		return $steps->{'done'}->($tl);
	    }

	    my $Label = Amanda::Label->new(tapelist => $tl,
					   user_msg => $user_msg);

	    my @labels;
	    if (exists $params{'remove_no_retention'}) {
		@labels = Amanda::Tapelist::list_no_retention();
	    } elsif ($params{'labels'}){
		@labels = split ',', $params{'labels'};
	    } elsif ($params{'LABEL'}){
		@labels = ($params{'LABEL'});
	    } else {
		push @result_messages, Amanda::Config::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code   => 1500015,
				severity => $Amanda::Message::ERROR);
		return $steps->{'done'}->();
	    }

	    my $storage = $params{'storage'} || $params{'STORAGE'};
	    $Label->erase(labels        => \@labels,
			  storage       => $storage,
			  cleanup       => $params{'cleanup'},
			  dry_run       => $params{'dry_run'},
			  erase         => $params{'erase'},
			  external_copy => $params{'external_copy'},
			  keep_label    => $params{'keep_label'},
			  finished_cb   => $steps->{'done'});
	};

	step done => sub {
	    my $err = shift;

	    push @result_messages, $err if $err;

	    $finished_cb->();
	};

    };


    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub add_label {
    my %params = @_;
    Amanda::Util::set_pname("Amanda::Rest::Storages::Labels");
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
        my $msg = shift;
        push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
	    my $tl = Amanda::Rest::Labels::init();
	    if ($tl->isa("Amanda::Message")) {
		return $steps->{'done'}->($tl);
	    }
	    $storage = Amanda::Storage->new(tapelist => $tl,
					    storage_name => $params{'STORAGE'});
	    if ($storage->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($storage);
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($chg);
	    }

	    my $Label = Amanda::Label->new(storage  => $storage,
					   tapelist => $tl,
					   user_msg => $user_msg);

	    $Label->label(slot    => $params{'slot'},
			  label   => $params{'label'},
			  meta    => $params{'meta'},
			  force   => $params{'force'},
			  barcode => $params{'barcode'},
			  storage => $params{'STORAGE'},
			  finished_cb => $steps->{'done'});
	};

	step done => sub {
	    my $err = shift;

	    $finished_cb->();
	};

    };

    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub update_label {
    my %params = @_;
    Amanda::Util::set_pname("Amanda::Rest::Storages::Labels");
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return \@result_messages if @result_messages;

    my $user_msg = sub {
	my $msg = shift;
	push @result_messages, $msg;
    };

    my $main = sub {
	my $finished_cb = shift;
	my $storage;
	my $chg;
	my $Label;

	my $steps = define_steps
	    cb_ref => \$finished_cb,
	    finalize => sub { $storage->quit() if defined $storage;
			      $chg->quit() if defined $chg };

	step start => sub {
            my $tl = Amanda::Rest::Labels::init();
	    if ($tl->isa("Amanda::Message")) {
		return $steps->{'done'}->($tl);
	    }

	    $storage = Amanda::Storage->new(storage_name => $params{'STORAGE'},
					    tapelist     => $tl);
	    if ($storage->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($storage);
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		return $steps->{'done'}->($chg);
	    }

	    $Label = Amanda::Label->new(storage  => $storage,
					tapelist => $tl,
					user_msg => $user_msg);


	    if (defined $params{'reuse'}) {
		my @labels = ($params{'LABEL'});
		if ($params{'reuse'}) {
		    $Label->reuse(labels      => \@labels,
				  finished_cb => $steps->{'assign'});
		} else {
		    $Label->no_reuse(labels      => \@labels,
				     finished_cb => $steps->{'assign'});
		}
	    } else {
		$steps->{'assign'}->();
	    }
	};

	step assign => sub {
	    if (defined $params{'meta'} || defined $params{'barcode'} ||
		defined $params{'pool'} || defined $params{'storage'}) {
		$Label->assign(label   => $params{'LABEL'},
			       meta    => $params{'meta'},
			       force   => $params{'force'},
			       barcode => $params{'barcode'},
			       pool    => $params{'pool'},
			       storage => $params{'storage'},
			       comment => $params{'comment'},
			       finished_cb => $steps->{'done'});
	    } else {
		$steps->{'done'}->();
	    }
	};

	step done => sub {
	    my $err = shift;

	    push @result_messages, $err if $err;

	    $finished_cb->();
	};

    };


    $main->(\&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
    $main = undef;

    return \@result_messages;
}

sub list {
    my %params = @_;

    Amanda::Util::set_pname("Amanda::Rest::Storages::Labels");
    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return @result_messages if @result_messages;

    my $tl = Amanda::Rest::Labels::init();
    if ($tl->isa("Amanda::Message")) {
	push @result_messages, $tl;
	return \@result_messages;
    }
    my @tles = @{$tl->{'tles'}};
    @tles = grep {defined $_->{'storage'}   and $_->{'storage'} eq $params{'STORAGE'}}                    @tles if $params{'STORAGE'};
    @tles = grep {                              $_->{'label'}   eq $params{'LABEL'}}                      @tles if $params{'LABEL'};
    @tles = grep {defined $_->{'config'}    and $_->{'config'}  eq $params{'config'}}                     @tles if $params{'config'};
    @tles = grep {defined $_->{'storage'}   and $_->{'storage'} eq $params{'storage'}}                    @tles if $params{'storage'};
    @tles = grep {defined $_->{'pool'}      and $_->{'pool'}    eq $params{'pool'}}                       @tles if $params{'pool'};
    @tles = grep {defined $_->{'meta'}      and $_->{'meta'}    eq $params{'meta'}}                       @tles if $params{'meta'};
    @tles = grep {                              $_->{'reuse'}   eq $params{'reuse'}}                      @tles if $params{'reuse'};
    @tles = grep {defined $_->{'datestamp'} and match_datestamp($params{'datestamp'}, $_->{'datestamp'})} @tles if defined $params{'datestamp'};
    push @result_messages, Amanda::Tapelist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code => 1600001,
				severity => $Amanda::Message::SUCCESS,
				tles => \@tles);
    return \@result_messages;
}

1;
