# vim:ft=perl
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

package Installcheck::Application;

use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Paths;
use Carp;
use Fcntl;
use IO::Handle;
use POSIX qw( :errno_h :fcntl_h );
use POSIX qw( EAGAIN );

use strict;
use warnings;

=head1 NAME

Installcheck::Application - driver Application API programs and scripts

=head1 SYNOPSIS

  use Installcheck::Application;

  my $app = Installcheck::Application->new('myapp');

  $app->add_property('foo' => 'bar');
  $app->add_property('baz');
  $app->add_property('bat', 1, 2, 3);
  $app->delete_property('foo');
  $app->get_property('foo');
  $app->set_property('foo', ['bar', '']);
  my @props = $app->list_properties();

  my $feats = $app->support('config' => 'TESTCONF', 'temp-prop' => 1);
  die "need calcsize support" unless ($feats{'CALCSIZE'});

  my $backup = $app->backup('device' => 'file:/path', 'level' => 0);
  die join("\n", @{$backup->{'errors'}}) if $backup->{'errors'};
  print STDERR join("\n", @{$backup->{'index'}});
  print $backup->{'data'};

=head1 USAGE

=over

=item C< new($app_name) >

Create a new C< Installcheck::Application > object that will run C< $app_name >.

=back

=head2 PROPERTIES

Properties are automatically passed as arguments to any command.
Their names will be lowercase.

=over

=item C< add_property($prop_name, 'val1', 'val2') >

Add one or more values for property C< $prop_name >.

=item C< delete_property($prop_name) >

Delete all values for property C< $prop_name >.

=item C< get_property($prop_name) >

Get all values for property C< $prop_name >.

=item C< set_property($prop_name, 'val1', 'val2') >

Set the values for property C< $prop_name >, removing any previous values.

=item C< list_properties() >

Returns all properties that have a value.

=back

=head2 COMMANDS

=over

=item C< support() >

Runs the C< support > command and returns the output as a hashref,
with all keys in uppercase.

=item C<< backup('disk' => '/some/path', 'device' => '/some/path, 'level' => 0) >>

Runs the C< backup() > command on the given device.
If a C< disk > argument is not given, it defaults to the C< device >.
Returns a hashref:

=over

=item C< data >

The data stream produced by the application

=item C< index >

An array of index lines produced by the application

=item C< size >

The size of the backup (C< data >)

=item C< info >

Any normal/informative messages

=item C< errors >

Any error messages

=item C< unknowns >

Any 'unknown' output

=item C< exit_status >

The exit status of the application

=back

=item C<< restore('data' => $data_from_backup, 'objects' => ['a', 'b'], 'level' => 0) >>

Runs the C< restore > command to restore the C< objects > to the
current working directory, supplying it with C< data >.
The optional C< level > argument (defaulting to 0) specifies the level of the backup
Returns a hashref:

=over

=item C< msgs >

Any output from the application

=item C< exit_status >

The exit status of the application

=back

=item C<< estimate('device' => '/some/path, 'level' => 0) >>

Returns a hashref:

=over

=item C< level >

The level of the backup that would result

=item C< size >

The size of the backup that would result

=item C< exit_status >

The exit status of the application

=back

=item C<< selfcheck('device' => '/some/path, 'disk' => '/some/path') >>

Runs the C< selfcheck > command on the given device.
If a C< disk > argument is not given, it defaults to the C< device >.)
Returns a hashref:

=over

=item C< oks >

OK messages

=item C< errors >

ERROR messages

=item C< exit_status >

The exit status of the application

=back

=back

=cut

sub new {
    my ($class, $app_name) = @_;

    my $self = {
        'app_name' => $app_name,
        'props' => {},
    };

    bless($self, $class);
    $self;
}

sub _propify {
    my $str = shift @_;
    $str = lc($str);
    $str =~ s/_/-/;
    $str;
}

sub add_property {
    my $self = shift @_;
    my $name = _propify(shift @_);
    my @values = @_;

    $self->{'props'}->{$name} ||= [];
    push @{$self->{'props'}->{$name}}, @values;
}

sub delete_property {
    my $self = shift @_;
    my $name = _propify(shift @_);

    delete $self->{'props'}->{$name};
}

sub get_property {
    my $self = shift @_;
    my $name = _propify(shift @_);

    defined($self->{'props'}->{$name}) ? @{$self->{'props'}->{$name}} : ();
}

sub set_property {
    my $self = shift @_;
    my $name = _propify(shift @_);
    my @values = @_;

    @{$self->{'props'}->{$name}} = @values;
}

sub list_properties {
    my $self = shift @_;

    my @prop_names = keys %{$self->{'props'}};
    # return only non-empty properties
    grep { $self->{'props'}->{$_} } @prop_names;
}

# setup and run the application
# $cmd - the command to give the application
# $extra_args - an arrayref of additional arguments
# $fds - a hashref of hashrefs specifying file descriptors
#   The key specifies the target file descriptor number in the child process.
#   Each hashref can have:
#     'child_mode' - 'r' or 'w', how the fd will be used in the child process
#     'cb' - an explicit callback to use when the fd is ready for reading/writing
#     'write' - a string to write out. An appropriate callback will be generated
#     'save_to' - a scalarref to save output to. An appropriate callback will be generated
#   For each key in $fds, a pipe will be setup.
#   Additional keys will be added:
#     'child_fd' - the file descriptor used by the child
#     'parent_fd' - the file descriptor used by the parent
#     'handle' - an anonymous filehandle (IO::Handle) for 'parent_fd'
#     'src' - the event source (for Amanda::MainLoop)
#     'done' - a callback (coderef) that must be called when you're done with the fd
# returns child exit status
sub _exec {
    my ($self, $cmd, $extra_args, $fds) = @_;
    confess 'must have a command' unless $cmd;

    my $fdn; # file descriptor number
    my $exit_status;

    my $all_done = sub {
        if (defined($exit_status)) {
            # check fds
            my $really_done = 1;
            foreach $fdn (keys %$fds) {
                my $fd = $fds->{$fdn};
                if (($fd->{'child_mode'} eq 'w') and ref($fd->{'done'})) {
                    $really_done = 0;
                    last;
                }
            }
            Amanda::MainLoop::quit() if $really_done;
        }
    };

    # start setting up pipes
    foreach $fdn (keys %$fds) {
        my $fd = $fds->{$fdn};
        confess "mode must be either 'r' or 'w'" unless $fd->{'child_mode'} =~ /^r|w$/;
        my ($fd0, $fd1) = POSIX::pipe();
        my ($c_fd, $p_fd, $p_mode);
        if ($fd->{'child_mode'} eq 'r') {
            $p_fd = $fd->{'parent_fd'} = $fd1;
            $p_mode = 'w';
            $c_fd = $fd->{'child_fd'} = $fd0;
        } else {
            $p_fd = $fd->{'parent_fd'} = $fd0;
            $p_mode = 'r';
            $c_fd = $fd->{'child_fd'} = $fd1;
        }

        my $p_handle = $fd->{'handle'} = IO::Handle->new_from_fd($p_fd, $p_mode);
        confess "unable to fdopen $p_fd with mode $p_mode" unless $p_handle;

        if ($fd->{'save_to'}) {
            $fd->{'cb'} = _make_save_cb($fd->{'save_to'}, $fd);
        } elsif ($fd->{'write'}) {
            $fd->{'cb'} = _make_write_cb($fd->{'write'}, $fd);
        }
        $fd->{'done'} = _make_done_cb($fd, $all_done);

        my $events = ($fd->{'child_mode'} eq 'r') ? $G_IO_OUT : ($G_IO_IN|$G_IO_HUP);
        $fd->{'src'} = Amanda::MainLoop::fd_source($p_handle, $events);
        $fd->{'src'}->set_callback($fd->{'cb'}) if $fd->{'cb'};
    }

    # build arguments
    $extra_args ||= [];
    my @args = ($cmd, @$extra_args);
    foreach my $name (keys %{$self->{'props'}}) {
        $self->{'props'}->{$name} ||= [];
        foreach my $val (@{$self->{'props'}->{$name}}) {
            push @args, "--$name", "$val";
        }
    }

    my $pid = fork();
    if ($pid) { # in parent
        # parent shouldn't use child_fd
        foreach $fdn (keys %$fds) {
            my $fd = $fds->{$fdn};
            POSIX::close($fd->{'child_fd'});
        }
        my $wait_src = Amanda::MainLoop::child_watch_source($pid);
        $wait_src->set_callback(sub {
            $exit_status = $_[2];
            $all_done->();
        });

        Amanda::MainLoop::run();

        # cleanup
        # don't need to remove wait_src, that's done automatically
        foreach $fdn (keys %$fds) {
            my $fd = $fds->{$fdn};
            $fd->{'src'}->remove();
            POSIX::close($fd->{'parent_fd'});
        }
    } else { # in child
        # juggle fd numbers
        my @child_fds = keys %$fds;
        foreach $fdn (@child_fds) {
            my $fd = $fds->{$fdn};
            confess "failed to call dup2($fd->{'child_fd'}, $fdn)"
                unless POSIX::dup2($fd->{'child_fd'}, $fdn);
            POSIX::close($fd->{'child_fd'})
                unless grep {$_ == $fd->{'child_fd'}} @child_fds;
            POSIX::close($fd->{'parent_fd'})
                unless grep {$_ == $fd->{'parent_fd'}} @child_fds;
        }

        # doesn't return
        exec "$APPLICATION_DIR/$self->{'app_name'}", @args;
    }

    $exit_status;
}

# given a fd hashref, make a callback that will make the fd non-blocking
sub _make_nonblock_cb {
    my $fd = shift @_;
    confess "a hash reference (representing a fd) is required" unless $fd;
    my $nonblock = 0;

    sub {
        unless ($nonblock) {
            my $flags = 0;
            fcntl($fd->{'handle'}, F_GETFL, $flags)
                or confess "Couldn't get flags: $!\n";
            $flags |= O_NONBLOCK;
            fcntl($fd->{'handle'}, F_SETFL, $flags)
                or confess "Couldn't set flags: $!\n";

            $nonblock = 1;
        }
    }
}

# given a scalar/string and a fd hashref,
# make a callback that will write the string to the fd
sub _make_write_cb {
    my ($data, $fd) = @_;
    confess "a hash reference (representing a fd) is required" unless $fd;
    my $len = length($data);
    my $offset = 0;
    my $nonblock_cb = _make_nonblock_cb($fd);

    my $BYTES_TO_WRITE = 4096;
    sub {
        my $src = shift @_;
        $nonblock_cb->();

        # this shouldn't happen since the src is removed once we're done (below)
        confess "offset greater than length" if $offset >= $len;

        my $rv = $fd->{'handle'}->syswrite($data, $BYTES_TO_WRITE, $offset);
        if (!defined($rv)) {
            confess "Error writing: $!" unless $! == EAGAIN;
        }
        $offset += $rv;

        $fd->{'done'}->() if ($offset >= $len);
    }
}


# given a scalarref and a fd hashref,
# make a callback that will save bytes from fd in scalarref
sub _make_save_cb {
    my ($store, $fd) = @_;
    confess "a scalar reference is required" unless ref($store) eq "SCALAR";
    confess "a hash reference (representing a fd) is required" unless $fd;
    $$store = '';
    my $offset = 0;
    my $nonblock_cb = _make_nonblock_cb($fd);

    my $BYTES_TO_READ = 4096;
    sub {
        my $src = shift @_;
        $nonblock_cb->();

        my $rv = $fd->{'handle'}->sysread($$store, $BYTES_TO_READ, $offset);
        if (defined($rv)) {
            $fd->{'done'}->() if (0 == $rv);
        } else {
            confess "Error reading: $!" unless $! == EAGAIN;
        }
        $offset += $rv;
    }
}

sub _make_done_cb {
    my ($fd, $all_done) = @_;
    sub {
        $fd->{'src'}->remove();
        $fd->{'handle'}->close();
        $fd->{'done'} = 1;
        $all_done->();
    }
}

# parse the size string output by various commands, returning the number of bytes
sub _parse_size {
    my $sstr = shift @_;

    confess "failed to parse size" unless ($sstr =~ /^(\d+)(\D?)$/i);
    my $size = 0 + $1;
    my $suf = lc($2);

    $suf = 'k' unless $suf;
    my %suf_pows = ('k' => 10, 'm' => 20, 'g' => 30);
    confess "invalid suffix $suf" unless $suf_pows{$suf};
    $size *= 1 << $suf_pows{$suf};

    $size;
}

sub support {
    my $self = shift @_;

    my $sup_str;
    _exec($self, 'support', undef, {
        1 => {'child_mode' => 'w', 'save_to' => \$sup_str},
    });

    my %sup = split(/\s+/, $sup_str);
    # fold into uppercase
    foreach my $k (keys %sup) {
        my $v = $sup{$k};
        delete $sup{$k};
        $sup{uc($k)} = $v;
    }

    \%sup;
}

sub backup {
    my $self = shift @_;
    my %nargs = @_;

    foreach my $k ( qw(device level) ) {
        confess "$k required" unless defined($nargs{$k});
    }
    $nargs{'disk'} ||=  $nargs{'device'};

    my @args = map {my $k = $_; ("--$k", $nargs{$k}) } keys(%nargs);

    my ($data, $msg_str, $index_str);
    my $exit_status = _exec($self, 'backup', \@args,
        {
            1 => {'child_mode' => 'w', 'save_to' => \$data},
            3 => {'child_mode' => 'w', 'save_to' => \$msg_str},
            4 => {'child_mode' => 'w', 'save_to' => \$index_str},
        }
    );

    my @index = split(/\n/, $index_str);


    # parse messages
    my ($size, @infos, @errors, @unknowns);
    foreach my $l (split(/\n/, $msg_str)) {
        if ($l =~ /^([|?&]) (.*)$/) {
            my ($sym, $rem) = ($1, $2);
            my $arr_ref;
            if ($sym eq '|') {
                push(@infos, $rem);
            } elsif ($sym eq '?') {
                push(@errors, $rem);
            } elsif ($sym eq '&') {
                push(@unknowns, $rem);
            } else {
                confess "should not be reached";
            }
        } elsif ($l =~ /^sendbackup: (.*)$/) {
            my $rem = $1;
            if ($rem =~ /^size (\d+)$/i) {
                $size = _parse_size($1);
            } elsif (lc($rem) eq 'end') {
                # do nothing
            } else {
                confess "failed to parse: $l";
            }
        } else {
            confess "failed to parse: $l";
        }
    }

    {'data' => $data, 'index' => \@index, 'size' => $size,
     'infos' => \@infos, 'errors' => \@errors, 'unknowns' => \@unknowns,
     'exit_status' => $exit_status};
}

sub restore {
    my $self = shift @_;
    my %args = @_;

    foreach my $k ( qw(objects data) ) {
        confess "'$k' required" unless defined($args{$k});
    }
    $args{'level'} ||= 0;

    my $msgs;
    my $exit_status = _exec($self, 'restore', ['--level', $args{'level'}, @{$args{'objects'}}], {
        0 => {'child_mode' => 'r', 'write' => $args{'data'}},
        1 => {'child_mode' => 'w', 'save_to' => \$msgs},
    });

    {'msgs' => $msgs, 'exit_status' => $exit_status};
}

# XXX: index?

sub estimate {
    my $self = shift @_;
    my %nargs = @_;

    foreach my $k ( qw(device level) ) {
        confess "$k required" unless defined($nargs{$k});
    }
    $nargs{'disk'} ||=  $nargs{'device'};
    my $calcsize = $nargs{'calcsize'};
    delete $nargs{'calcsize'};

    my @args = map {my $k = $_; ("--$k", $nargs{$k}) } keys(%nargs);
    push @args, '--calcsize' if $calcsize;

    my $est;
    my $exit_status = _exec($self, 'estimate', \@args,
        {
            1 => {'child_mode' => 'w', 'save_to' => \$est},
        }
    );
    $est =~ /^(\d+) (\d+) 1\n$/;
    my ($level, $size) = ($1, $2);
    $level = 0 + $level;
    $size = ($size eq '-1')? -1 : _parse_size($size);

    {'level' => $level, 'size' => $size, 'exit_status' => $exit_status};
}

sub selfcheck {
    my $self = shift @_;
    my %nargs = @_;

    foreach my $k ( qw(device) ) {
        confess "$k required" unless defined($nargs{$k});
    }
    $nargs{'disk'} ||=  $nargs{'device'};

    my @args = map {my $k = $_; ("--$k", $nargs{$k}) } keys(%nargs);

    my $msg_str;
    my $exit_status = _exec($self, 'selfcheck', \@args,
        {
            1 => {'child_mode' => 'w', 'save_to' => \$msg_str},
        }
    );

    my (@oks, @errors);
    foreach my $l (split(/\n/, $msg_str)) {
        confess "invalid line: $l" unless $l =~ /^(OK|ERROR) (.*)$/;
        my ($type, $rem) = ($1, $2);
        if ($type eq 'OK') {
            push(@oks, $rem);
        } elsif ($type eq 'ERROR') {
            push(@errors, $rem);
        } else {
            confess "should not be reached";
        }
    }

    {'oks' => \@oks, 'errors' => \@errors, 'exit_status' => $exit_status};
}

# XXX: print?

1;
