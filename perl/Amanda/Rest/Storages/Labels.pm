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
use Amanda::Rest::Configs;
use Symbol;
use Data::Dumper;
use vars qw(@ISA);

=head1 NAME

Amanda::Rest::Stroages::Labels -- Rest interface to Amanda::Label

=head1 INTERFACE

=over

=item Amanda::Rest::Storages::Labels::assign

Interface to C<Amanda::Rest::Stoages::labels::assign>
Assign meta, barcode, pool and/or storage to a label.

  {"jsonrpc":"2.0",
   "method" :"Amanda::Rest::Storages::Labels::assign",
   "params" :{"config":"test",
	      "label":"$label"
	      "meta":"$meta"
	      "barcode":"$barcode"
	      "pool":"$pool"
              "storage":"$storage"},
   "id"     :"1"}

Do not set "meta", "barcode", "pool" or "storage" if you do not want to modify them, set them to an empty string "" if you want to unset them.

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"Amanda/Label.pm",
              "source_line":"298",
              "code":"1000006",
	      "message":"Setting $label"}],
   "id":"1"}

=item Amanda::Rest::Label::label

Interface to C<Amanda::label::label>
Label a volume.

  {"jsonrpc":"2.0",
   "method" :"Amanda::Rest::Label::label",
   "params" :{"config":"test",
	      "slot":"$slot"
	      "label":"$label"
	      "meta":"$meta"
	      "barcode":"$barcode"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "source_line":"404",
	      "code":1000008,
	      "message":"Reading label..."},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "source_line":"465",
	      "code":1000009,
	      "message":"Found an empty tape."},
	      {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "label":"$label",
	      "source_line":"598",
	      "code":1000020,
	      "message":"Writing label '$label'..."},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "source_line":"618",
	      "code":1000021,
	      "message":"Checking label..."},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "source_line":"660",
	      "code":1000022,
	      "message":"Success!"}],
   "id":"1"}


=item Amanda::Rest::Label::erase

Interface to C<Amanda::label::erase>
Erase a volume.

  {"jsonrpc":"2.0",
   "method" :"Amanda::Rest::Label::erase",
   "params" :{"config":"test",
	      "labels":["$label1","$label2"],
	      "cleanup":"$cleanup"
	      "dry_run":"$dry_run"
	      "erase":"$erase"
	      "keep_label":"$keep_label"},
   "id"     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "label":"test-ORG-AA-vtapes-001",
	      "source_line":"878",
	      "code":1000049,
	      "message":"Erased volume with label 'test-ORG-AA-vtapes-001'."},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "label":"test-ORG-AA-vtapes-001",
	      "source_line":"891",
	      "code":1000050,"message":
	      "Rewrote label 'test-ORG-AA-vtapes-001' to volume."},
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "tapelist_filename":"/amanda/h1/etc/amanda/test/tapelist",
	      "label":"test-ORG-AA-vtapes-001",
	      "source_line":"971",
	      "code":1000052,
	      "message":"Removed label 'test-ORG-AA-vtapes-001 from tapelist file."}],
   "id":"1"}

=item Amanda::Rest::Label::reuse

Interface to C<Amanda::label::reuse>
Erase a volume.

  {"jsonrpc":"2.0",
   "method" :"Amanda::Rest::Label::reuse",
   "params" :{"config":"test",
	      "labels":["$label1","$label2"]},
   "id:     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "label":"$label1:,
	      "source_line":"1180",
	      "code":1000045,
	      "message":"marking tape '$label1' as reusable."}]
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "label":"$label2",
	      "source_line":"1180",
	      "code":1000045,
	      "message":"marking tape '$label2' as reusable."}]
   "id:     :"1"}

=item Amanda::Rest::Label::no_reuse

Interface to C<Amanda::label::no_reuse>
Erase a volume.

  {"jsonrpc":"2.0",
   "method" :"Amanda::Rest::Label::no_reuse",
   "params" :{"config":"test",
	      "labels":["$label1","$label2"]},
   "id:     :"1"}

The result is an array of Amanda::Message:

  {"jsonrpc":"2.0",
   "result":[{"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "label":"$label1:,
	      "source_line":"1256",
	      "code":1000047,
	      "message":"marking tape '$label1' as not reusable."}]
	     {"source_filename":"/amanda/h1/linux/lib/amanda/perl/Amanda/Label.pm",
	      "label":"$label2",
	      "source_line":"1256",
	      "code":1000047,
	      "message":"marking tape '$label2' as not reusable."}]
   "id:     :"1"}

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
				tapefile => $filename);
    }
    return $tl;
}

sub label {
    my %params = @_;
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
		return $steps->done($tl);
	    }

	    $storage = Amanda::Storage->new(storage_name => $params{'STORAGE'},
					    tapelist     => $tl);
	    if ($storage->isa("Amanda::Changer::Error")) {
		push @result_messages, $storage;
		return $steps->done();
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		push @result_messages, $chg;
		return $steps->done();
	    }

	    my $Label = Amanda::Label->new(storage  => $storage,
					   tapelist => $tl,
					   user_msg => $user_msg);

	    $Label->label(slot    => $params{'slot'},
			  label   => $params{'label'},
			  meta    => $params{'meta'},
			  force   => $params{'force'},
			  barcode => $params{'barcode'},
			  finished_cb => $steps->{'done'});
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

sub erase {
    my %params = @_;
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
				message => "Errors processing disklist");
		$steps->done();
	    }

            my $tl = Amanda::Rest::Labels::init();
	    if ($tl->isa("Amanda::Message")) {
		return $steps->done($tl);
	    }

	    my $Label = Amanda::Label->new(tapelist => $tl,
					   user_msg => $user_msg);

	    my @labels;
	    if ($params{'remove_no_retention'}) {
		@labels = Amanda::Tapelist::list_no_retention();
	    } else {
		@labels = ($params{'LABEL'});
	    }

	    $Label->erase(labels      => \@labels,
			  cleanup     => $params{'cleanup'},
			  dry_run     => $params{'dry_run'},
			  erase       => $params{'erase'},
			  keep_label  => $params{'keep_label'},
			  finished_cb => $steps->{'done'});
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

sub post_label {
    my %params = @_;
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
		return $steps->done($tl);
	    }

	    $storage = Amanda::Storage->new(storage_name => $params{'STORAGE'},
					    tapelist     => $tl);
	    if ($storage->isa("Amanda::Changer::Error")) {
		return $steps->done($storage);
	    }

	    $chg = $storage->{'chg'};
	    if ($chg->isa("Amanda::Changer::Error")) {
		return $steps->done($chg);
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

    my @result_messages = Amanda::Rest::Configs::config_init(@_);
    return @result_messages if @result_messages;

    my $tl = Amanda::Rest::Labels::init();
    if ($tl->isa("Amanda::Message")) {
	return $tl;
    }
    my @new_tles = grep {$_->{'storage'} eq $params{'STORAGE'}} @{$tl->{'tles'}};
    return Amanda::Tapelist::Message->new(
				source_filename => __FILE__,
				source_line     => __LINE__,
				code => 1600001,
				tles => \@new_tles);
}

1;
