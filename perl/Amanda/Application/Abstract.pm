# This copyright apply to all codes written by authors that made contribution
# made under the BSD license.  Read the AUTHORS file to know which authors made
# contribution made under the BSD license.
#
# The 3-Clause BSD License

# Copyright 2017 Purdue University
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

use strict;
use warnings;

=head1 NAME

Amanda::Application::Abstract - A base class for Amanda applications

=head1 SYNOPSIS

    package amMyAppl;
    use base qw(Amanda::Application::Abstract)
    
    sub supports_message_line { return 1; }
    ...
    
    sub inner_backup {
      my ($self, $fdout) = @_;
      ...
    }
    ...
    
    package main;
    amMyAppl::->run();

=head1 DESCRIPTION

C<Amanda::Application::Abstract> handles much of the common housekeeping
needed to implement Amanda's
L<Application API|http://wiki.zmanda.com/index.php/Application_API>, so that
practical applications can be implemented in few lines of code by overriding
only a few necessary methods.

C<Amanda::Application::Abstract> is itself a subclass of
C<Amanda::Application>, but the latter cannot be instantiated until after
the command line has been parsed (at least the C<config> option needs to be
passed to its constructor). Therefore, C<Amanda::Application::Abstract>
supplies I<class> methods for the preliminary work of declaring the supported
options, parsing the command line, and finally calling C<new> to get an instance
of the class. Then the actual operations of the application are carried out by
instance methods, as you would expect.

In perl, class methods are inheritable and overridable just as instance methods
are, and the syntax C<$class-E<gt>SUPER::method(...)> works, analogously to its
familiar use in an instance method. So, the pre-instantiation behavior of an
application (declaring command options, etc.) can be tailored by simply
overriding the necessary class methods, just as its later operations can be
supplied by overriding instance methods.

=cut

package Amanda::Application::Abstract;
use base qw(Amanda::Application);

use Amanda::Feature;
use Data::Dumper;
use File::Path qw{make_path};
use File::Spec;
use Getopt::Long;
use IO::Handle;
use Scalar::Util qw{blessed};
use Text::ParseWords;

=head1 FUNDAMENTAL CLASS METHODS

=head2 C<run>

    appclass::->run()

Run the application: grab the subcommand from C<ARGV[0]>, be sure it is a known
one, set up for option parsing, parse the options, construct an instance
passing all the supplied options (which will include the config name, needed
by the C<Amanda::Application> constructor), and finally C<do()> the subcommand,
where C<do> is inherited from C<Amanda::Script_App> by way of
C<Amanda::Application>.

=cut

sub run {
    my ( $class ) = @_;
    my $subcommand = shift @ARGV || '(empty)';
    die "Not a possible application subcommand: ".$subcommand
    if $subcommand !~ /^(?:backup|discover|estimate|index|print|restore|selfcheck|support|validate)/;

    my %opthash = ();
    my @optspecs = ();

    my $optinit = $class->can('declare_'.$subcommand.'_options');
    die "Unsupported application subcommand: ".$subcommand if !defined $optinit;

    $class->$optinit(\%opthash, \@optspecs);

    my $ret = GetOptions(\%opthash, @optspecs);

    # ??? Application API docs suggest that escaping can have been applied to
    # option values, in which case here is where it should be unwrapped!
    # First get signs of life, then test with trick values to determine
    # exactly how values have been escaped.

    # !!! pleasant surprise: option value arrived with escaping already
    # unwrapped. Examining sendbackup debug log shows the value apparently
    # passed to sendbackup in an escaped form, but unescaped in the section
    # of the log file that shows "sendbackup: running ..." with the arguments
    # logged one by one. Looks like sendbackup does the unwrapping and uses
    # exec directly; todo: check the code to confirm.

    # ... yes, that's just what sendbackup does.

    my $app = $class->new(\%opthash);

    $app->do($subcommand);
}

=head2 C<new>

    $class->new(\%opthash)

A typical application need not call this; it is called by C<run> after the
command line options have been declared and the command line has been parsed.
The hash reference contains option values as stored by C<GetOptions>. A certain
convention is assumed: if there is an option/property named I<o>, its value
will be stored in I<opthash> with key exactly I<o>, in exactly the way
C<GetOptions> normally would, I<unless> the option-declaring method had planted
a code reference there in order to more strictly validate the option. In that
case, of course I<$opthash{o}> is the code reference, and the final value will
be stored with key C<opt_>I<o> instead. Because of that convention, entries in
I<opthash> with an C<opt_> prefix will otherwise be ignored; avoid declaring
options/properties whose actual names start with C<opt_>.

=cut

sub new {
    my ( $class, $refopthash ) = @_;
    my %options = ();
    for ( my ($k, $v) ; ($k, $v) = each %{$refopthash} ; ) {
	next if $k =~ /^opt_/;
	if ( "CODE" ne ref $v ) {
	    $options{$k} = $v;
	}
	else {
	    $options{$k} = $refopthash->{"opt_".$k}
	}
    }
    my $self = $class->SUPER::new($options{'config'});
    $self->{'options'} = \%options;
    return $self;
}

=head1 CLASS METHODS IMPLEMENTING VARIOUS QUOTING RULES

=head2 C<dquote>

    $class->dquote($s)

The purpose of this method is to quote a string in exactly the way that
C<Text::ParseWords::shellquote> unquotes things, because C<shellquote> is what
C<GetOptionsFromString> claims to use. However, the
L<Text::ParseWords POD|Text::ParseWords> does not spell out the rules to use.
O'Reilly's Perl Cookbook says (1.15) that "Quotation marks and backslashes are
the only characters that have meaning backslashed. Any other use of a backslash
will be left in the output string." ... which seems to be true, looking at the
code, and is true of ' when ' is the quote mark, and true of " when " is the
quote mark. That makes it actually I<not> like any common Unix shell, and also
not like C<Amanda::Util::quote_string>, so it needs its own dedicated
implementation here to be sure to get it right.

=cut

sub dquote {
    my ( $class, $s ) = @_;
    return $s if 1 eq scalar shellwords($s);
    my $qs = $s;
    $qs =~ s/([\\"])/\\$1/go;
    $qs = '"' . $qs . '"';
    my @parsed = shellwords($qs);
    return $qs if 1 eq scalar @parsed and $s eq $parsed[0];
    die 'dquote could not handle: ' . $s;
}

=head2 C<gtar_escape_quote>

    $class->gtar_escape_quote($s)

This method quotes a string using the same rules documented for
GNU C<tar> in C<--quoting-style=escape> mode, as used on the index stream.
(Note: the GNU C<tar> documentation, as of April 2016 anyway, says that space
characters also get escaped, though GNU C<tar> has never actually done that,
and neither does this method.)

=cut

sub gtar_escape_quote {
    my ( $class, $str ) = @_;
    $str =~ s'
        (\a)(?{"a"})|(\f)(?{"f"})|(\n)(?{"n"})|(\r)(?{"r"})|(\t)(?{"t"})
    |   (\010)(?{"b"})|(\013)(?{"v"})|(\177)(?{"?"})|(\\)(?{"\\"})
    |   ([[:cntrl:]])(?{sprintf("%03o",ord($^N))})
    'q{\\}.$^R'egox;
    return $str;
}

=head2 C<gtar_escape_unquote>

    $class->gtar_escape_unquote($s)

The inverse of C<gtar_escape_quote>.

=cut

sub gtar_escape_unquote {
    my ( $class, $str ) = @_;
    $str =~ s'\\(?:
        (a)(?{"\a"})|(f)(?{"\f"})|(n)(?{"\n"})|(r)(?{"\r"})|(t)(?{"\t"})
    |   (b)(?{"\010"})|(v)(?{"\013"})|(\?)(?{"\177"})|(\\)(?{"\\"})
    |   ([0-7]{1,3})(?{chr(oct($^N))})
    )'$^R'egox;
    return $str;
}

=head1 CLASS METHODS FOR VALIDATING OPTION/PROPERTY VALUES

    ..._property_setter(\%opthash)

For a common type ... return a sub that can be placed in C<%opthash>
to validate and store a value of that type. The anonymous sub accepts the
parameters described at "User-defined subroutines to handle options" for
C<Getopt::Long>, and will store the validated, converted option value in
I<%opthash> at a key derived by prefixing C<opt_> to the option name.
For example, a C<declare_common_options> method that defines a boolean
property I<foo> may contain the snippet:

    $refopthash->{'foo'} = $class->boolean_property_setter($refopthash);

At present, properties with scalar or hash values can be supported (the
setter sub can tell by the number of parameters passed to it by
C<Getopt::Long>), but not multiple-valued ones accumulating into an array
(which can't be distinguished from the scalar case just by looking at what
comes from C<Getopt::Long>).

=head2 C<boolean_property_setter>

Allows the same boolean literals described in C<amanda.conf(5)>:
C<1>, C<y>, C<yes>, C<t>, C<true>, C<on> or
C<0>, C<n>, C<no>, C<f>, C<false>, C<off>, case-insensitively.

=cut

sub boolean_property_setter {
    my ( $class, $refopthash ) = @_;
    return sub {
	my ( $optname, $k, $v, $b );
	$optname = shift;
	$k = shift if 2 == scalar(@_);
	$v = shift;
	if ( $v =~ /^(?:1|y|yes|t|true|on)$/i ) {
            $b = 1;
	}
	elsif ( $v =~ /^(?:0|n|no|f|false|off)$/i ) {
	    $b = 0;
	}
	else {
	    die 'invalid value ' . Amanda::Util::quote_string($v) .
	        ' for boolean property ' .
	        Amanda::Util::quote_string("$optname");
	}
	if ( defined $k ) {
	    $refopthash->{'opt_'.$optname}->{$k} = $b;
	}
	else {
	    $refopthash->{'opt_'.$optname} = $b;
	}
    };
}

=head1 CLASS METHODS FOR DECLARING ALLOWED OPTIONS

    declare_..._options(\%opthash, \@optspecs)

Each of these is a class method that is passed two references: a hash reference
I<\%opthash> and a list reference I<\@optspecs>. It should push onto
I<\@optspecs> the declarations of whatever options are valid for the
corresponding subcommand, using the syntax described in L<Getopt::Long>.
It does not need to touch I<\%opthash>, but may store a code reference at the
name of any option, to apply stricter validation when the option is parsed.
In that case, the code reference should ultimately store the validated value
into I<\%opthash> at a key derived by prefixing C<opt_> to the option name.

For each valid I<subcommand>, there must be a
C<declare_>I<subcommand>C<_options> class method, which will be called
by C<run> just before trying to parse the command line.

=head2 C<declare_common_options>

Declare the options common to every subcommand. This is called by
every other C<declare_>I<foo>C<_options> method, and, if not overridden itself,
simply declares the C<config>, C<host>, C<disk>, C<device> options that
(almost?) every subcommand ought to support.

A subclass that has properties of its own should probably declare them here
rather than in more-specific C<declare_>I<foo>C<_options> methods, as whatever
properties are set in the Amanda configuration could be passed along for any
subcommand; Amanda won't know which subcommands need them and which don't.

=cut

sub declare_common_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    push @$refoptspecs, ( 'config=s', 'host=s', 'disk=s', 'device=s' );
}

=head2 C<declare_support_options>

Declare options for the C<support> subcommand. Only the C<common> ones.

=cut

sub declare_support_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->declare_common_options($refopthash, $refoptspecs);
}

=head2 C<declare_backup_options>

Declare options for the C<backup> subcommand. Unless overridden, this simply
declares the C<common> ones plus C<message>, C<index>, C<level>, C<record>, and
C<state-stream>.

=cut

sub declare_backup_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, (
        'message=s', 'index=s', 'level=i', 'record', 'state-stream=i' );
}

=head2 C<declare_restore_options>

Declare options for the C<restore> subcommand. Unless overridden, this simply
declares the C<common> ones plus C<message>, C<index>, C<level>, C<dar>, and
C<recover-dump-state-file>.

=cut

sub declare_restore_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, (
        'message=s', 'index=s', 'level=i',
	'dar=s', 'recover-dump-state-file=s' );
    $refopthash->{'dar'} = sub {
        my ( $optname, $optval ) = @_;
	if ( $optval eq 'YES' ) {
	    $refopthash->{'opt_'.$optname} = 1;
	}
	elsif ( $optval eq 'NO' ) {
	    $refopthash->{'opt_'.$optname} = 0;
	}
	else {
	    die "--$optname requires YES or NO";
	}
    };
}

=head2 C<declare_index_options>

Declare options for the C<index> subcommand. Unless overridden, this simply
declares the C<common> ones plus C<message>, C<index>, C<level>.

=cut

sub declare_index_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, (
        'message=s', 'index=s', 'level=i' );
}

=head2 C<declare_estimate_options>

Declare options for the C<estimate> subcommand. Unless overridden, this simply
declares the C<common> ones plus C<message> and C<level>.
If C<$class-E<gt>supports('multi_estimate')> then C<level> will be declared to
allow multiple uses.

=cut

sub declare_estimate_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, (
        'message=s', 'level=i'.($class->supports('multi_estimate') ? '@' : '' )
    );
}

=head2 C<declare_selfcheck_options>

Declare options for the C<selfcheck> subcommand. Unless overridden, this simply
declares the C<common> ones plus C<message>, C<level>, and C<record>.

=cut

sub declare_selfcheck_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, ('message=s', 'index=s', 'level=i', 'record');
}

=head2 C<declare_validate_options>

Declare options for the C<validate> subcommand. Unless overridden, this
declares no options at all.

=cut

sub declare_validate_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
}

=head1 CLASS METHODS SUPPORTING C<support> SUBCOMMAND

These class methods establish the default capabilities advertised by the
C<support> subcommand. A particular application can supply I<class> methods
to override them. Most are boolean and an absent one is treated as returning
C<false>. Therefore, the only ones that need to be supplied here are the
non-booleans, and the booleans with non-C<false> defaults.

=head2 C<recover_mode>

Should return either C<undef> or C<'SMB'>. Default if not overridden
is C<undef>.

=cut

sub recover_mode { my ( $class ) = @_; return undef; }

=head2 C<data_path>

Should return a list of C<'AMANDA'>, C<'DIRECTTCP'>, or both.
Default if not overridden is C<'AMANDA'>.

=cut

sub data_path { my ( $class ) = @_; return ("AMANDA"); }

=head2 C<recover_path>

Should return C<'CWD'> or C<'REMOTE'>. Default if not overridden is C<CWD>.

=cut

sub recover_path { my ( $class ) = @_; return "CWD"; }

=head2 C<supports>

    $class->supports(name)

Returns C<false> if there is no C<supports_>I<name>C<()> class method,
otherwise calls it and returns its result.

=cut

sub supports {
    my ( $class, $supname ) = @_;
    my $s = $class->can("supports_".$supname);
    return (defined $s) and $s;
}

=head1 INSTANCE METHODS SUPPORTING C<support> SUBCOMMAND

=head2 C<max_level>

Returns the maximum C<level> supported, Default if not overridden is zero,
indicating no support for incremental backup. An instance method because the
value could depend on state only known after instantiation.

=cut

sub max_level { my ( $self ) = @_; return 0; }

#
# Private instance method to produce a support-line for a boolean
# capability:  $self->say_supports($confstring, $supname) where $confstring
# is the text to output on fd1, followed by a space and YES or NO and a
# newline, as determined by calling $class->supports($supname).
#

my $say_supports = sub {
    my ( $self, $confstring, $supname ) = @_;
    my $yn = blessed($self)->supports($supname) ? "YES" : "NO";
    print $confstring . " " . $yn . "\n";
};

#
# Private instance methods used only to sanity-check return values in case
# certain "supports" methods are overridden:
#

# recover_mode (class method) should return either undef or "SMB"
my $checked_recover_mode = sub {
    my ( $self ) = @_;
    my $rm = blessed($self)->recover_mode();
    die "unusable recover_mode: ".$rm if defined $rm and $rm ne "SMB";
    return $rm;
};

# data_path (class method) should return list: "AMANDA", "DIRECTTCP", or both
my $checked_data_path = sub {
    my ( $self ) = @_;
    my @dp = blessed($self)->data_path();
    my $dpl = scalar @dp;
    die "unusable data_path: ".Dumper(\@dp)
        if $dpl lt 1 or 0 lt grep(!/^(?:AMANDA|DIRECTTCP)$/, @dp);
    return @dp;
};

# recover_path (class method) should return either "CWD" or "REMOTE"
my $checked_recover_path = sub {
    my ( $self ) = @_;
    my $rp = blessed($self)->recover_path();
    die "unusable recover_path: ".$rp
        unless defined $rp and $rp =~ /^(?:CWD|REMOTE)$/;
    return $rp;
};

# max_level (instance method) should return 0..99
my $checked_max_level = sub {
    my ( $self ) = @_;
    my $mxl = $self->max_level();
    die "unusable max_level: ".$mxl unless defined $mxl and $mxl =~ /^\d\d?$/;
    return 0 + $mxl;
};

=head1 INSTANCE METHODS SUPPORTING LOCAL PERSISTENT STATE

These methods establish a conventional location for a local state file,
and a default format for its contents. To avoid relying on non-core modules
(even JSON isn't a sure thing yet), the default format relies on writing
name/value pairs to the file in a form C<Getopt::Long> can recover.

By default, a state is a hash with integer keys representing levels, plus one
string key, C<maxlevel>. The value for C<maxlevel> is an integer, and for each
integer key I<level>, the value is a hashref that contains
(redundantly) C<'level' =E<gt> >I<level> plus whatever other name/value pairs
the application wants.

=head2 C<local_state_path>

    ($dirpart, $filepart) = $self->local_state_path()

Supplies a reasonable location for storing local state for the subject DLE.
If not overridden, returns a I<$dirpart> based on
C<$Amanda::Paths::localstatedir> followed by C<Amanda::Util::hexencode>d
components based on C<config>, C<host>, C<disk>, and C<device> if present, and a
I<$filepart> based on the application name.

=cut

sub local_state_path {
    my ( $self ) = @_;
    return $self->build_state_path($Amanda::Paths::localstatedir);
}

=head2 C<build_state_path>

    ($dirpart, $filepart) = $self->build_state_path($basedir)

Does the actual building of a local state path (as described above for
C<local_state_path>) given a base directory, so that a subclass can easily
override C<local_state_path> to use a different base directory, but otherwise
build the rest of the path the same way.

=cut

sub build_state_path {
    my ( $self, $basedir ) = @_;
    my @components = ( $basedir, $self->{'type'} );
    my $c = $self->{'options'}->{'config'};
    push @components, 'c-'.Amanda::Util::hexencode($c) if defined $c;
    $c = $self->{'options'}->{'host'};
    push @components, 'h-'.Amanda::Util::hexencode($c) if defined $c;
    $c = $self->{'options'}->{'disk'};
    push @components, 'd-'.Amanda::Util::hexencode($c) if defined $c;
    $c = $self->{'options'}->{'device'};
    push @components, 'v-'.Amanda::Util::hexencode($c) if defined $c;
    return File::Spec->catdir(@components),
        Amanda::Util::hexencode($self->{'name'});
}

=head2 C<read_local_state>

    $hashref = $self->read_local_state(\@optspecs)

Read a local state, if present, returning it in I<$hashref>. In the returned
state, the value for C<maxlevel> represents the maximum level of incremental
I<backup> that could be requested, namely, one plus the maximum previous level
represented in the state. If no stored state is found, a state hash is returned
with C<maxlevel> of zero and no other data. Otherwise, the file is parsed for
one or more levels of saved state, by repeatedly applying C<Getopt::Long> with
I<\@optspecs> to declare the application's allowed options.

=cut

sub read_local_state {
    my ( $self, $optspecs ) = @_;
    my ( $dirpart, $filepart ) = $self->local_state_path();
    my $ret = open my $fh, '<', File::Spec->catfile($dirpart, $filepart);
    if ( ! $ret ) {
    	my %empty = ( 'maxlevel' => 0 );
	return \%empty;
    }
    my $slurped;
    {
        local $/;
	$slurped = <$fh>;
    }
    close $fh;
    my $opthash = {};
    my $maxlevel = 0;
    my %result;
    my $remain;
    ( $ret, $remain ) =
        Getopt::Long::GetOptionsFromString($slurped, $opthash, @$optspecs);
    # XXX carp if remain[0] doesn't start with --
    my $lev = $opthash->{'level'};
    $maxlevel = $lev if $lev > $maxlevel;
    $result{$lev} = $opthash;
    while ( 0 < scalar @$remain ) {
        $opthash = {};
	$ret = Getopt::Long::GetOptionsFromArray($remain, $opthash, @$optspecs);
	$lev = $opthash->{'level'};
        $maxlevel = $lev if $lev > $maxlevel;
        $result{$lev} = $opthash;
    }
    $result{'maxlevel'} = 1 + $maxlevel;
    return \%result;
}

=head2 C<write_local_state>

    $self->write_local_state(\%levhash)

Save the local state represented by I<\%levhash>, creating the file and
intermediate directories if necessary. C<Amanda::Util::safe_overwrite_file>
is used to avoid leaving partially-overwritten state.

=cut

sub write_local_state {
    my ( $self, $levhash ) = @_;
    my $state = '';
    for ( my ($level, $opthash); ($level, $opthash) = each %$levhash; ) {
        next if 'maxlevel' eq $level;
	$state .= '--level ' . $level . "\n";
	for ( my ($k, $v); ($k, $v) = each %$opthash; ) {
	    next if 'level' eq $k;
	    $state .= $self->dquote('--'.$k).' '.$self->dquote($v)."\n";
	}
	$state .= "--\n";
    }
    my ( $dirpart, $filepart ) = $self->local_state_path();
    make_path($dirpart);
    Amanda::Util::safe_overwrite_file(File::Spec->catfile($dirpart, $filepart),
                                      $state);
}

=head2 C<update_local_state>

    $self->update_local_state(\%state, $level, \%opthash)

Given a I<\%state> (which contains C<'maxlevel' =E<gt> >(I<n>+1) as well as
I<k>C< =E<gt> >I<(opthash for level k)> for I<k> in 0..n), and a new
I<$level> and corresponding I<\%opthash> for that level, change
C<maxlevel> to 1 + I<$level>, drop all entries for levels above
I<$level>, and install the new I<\%opthash> at I<$level>.

=cut

sub update_local_state {
    my ( $self, $state, $level, $opthash ) = @_;
    for ( my ($l, $oh); ($l, $oh) = each %$state; ) {
        next if 'maxlevel' eq $l or (0 + $l) le $level;
	delete $state->{$l};
    }
    $state->{'maxlevel'} = 1 + $level;
    $state->{$level} = $opthash;
}

=head1 INSTANCE METHODS IMPLEMENTING SUBCOMMANDS

=head2 C<command_support>

As long as the application class overrides the methods indicating its
support for these capabilities, this default implementation will take care
of producing output in the proper format.

=cut

sub command_support {
    my ( $self ) = @_;

    $self->$say_supports(	    "CONFIG", "config");
    $self->$say_supports(	      "HOST", "host");
    $self->$say_supports(	      "DISK", "disk");
    $self->$say_supports(	"INDEX-LINE", "index_line");
    $self->$say_supports(	 "INDEX-XML", "index_xml");
    $self->$say_supports(     "MESSAGE-LINE", "message_line");
    $self->$say_supports(      "MESSAGE-XML", "message_xml");
    $self->$say_supports(	    "RECORD", "record");
    $self->$say_supports(     "INCLUDE-FILE", "include_file");
    $self->$say_supports(     "INCLUDE-LIST", "include_list");
    $self->$say_supports("INCLUDE-LIST-GLOB", "include_list_glob");
    $self->$say_supports( "INCLUDE-OPTIONAL", "include_optional");
    $self->$say_supports(     "EXCLUDE-FILE", "exclude_file");
    $self->$say_supports(     "EXCLUDE-LIST", "exclude_list");
    $self->$say_supports("EXCLUDE-LIST-GLOB", "exclude_list_glob");
    $self->$say_supports( "EXCLUDE-OPTIONAL", "exclude_optional");
    $self->$say_supports(	"COLLECTION", "collection");
    $self->$say_supports(	  "CALCSIZE", "calcsize");
    $self->$say_supports(  "CLIENT-ESTIMATE", "client_estimate");
    $self->$say_supports(   "MULTI-ESTIMATE", "multi_estimate");

    print "MAX-LEVEL ".($self->$checked_max_level())."\n";

    my $rcvrmode = $self->$checked_recover_mode();
    print "RECOVER-MODE ".$rcvrmode."\n" if defined $rcvrmode;

    for my $dp ($self->$checked_data_path()) {
        print "DATA-PATH ".$dp."\n";
    }

    print "RECOVER-PATH ".($self->$checked_recover_path())."\n";

    $self->$say_supports(             "AMFEATURES", "amfeatures");
    $self->$say_supports("RECOVER-DUMP-STATE-FILE", "recover_dump_state_file");
    $self->$say_supports(                    "DAR", "dar");
    $self->$say_supports(           "STATE-STREAM", "state_stream");
}

=head2 METHODS FOR THE C<backup> SUBCOMMAND

=head3 C<check_message_index_options>

Check sanity of the (standard) options parsed from the command line
for C<--message> and C<--index>.

=cut

sub check_message_index_options {
    my ( $self ) = @_;

    my $msg = $self->{'options'}->{'message'};
    if ( ! defined $msg and $self->supports('message_line') ) {
        $self->{'options'}->{'message'} = 'line';
    }
    elsif ( 'line' eq $msg and $self->supports('message_line') ) {
        # jolly
    }
    elsif ( 'xml' ne $msg or ! $self->supports('message_xml') ) {
        $self->print_to_server('invalid --message '.$msg,
	                       $Amanda::Script_App::ERROR);
    }

    my $idx = $self->{'options'}->{'index'};
    if ( ! defined $idx and $self->supports('index_line') ) {
        $self->{'options'}->{'index'} = 'line';
    }
    elsif ( 'line' eq $idx and $self->supports('index_line') ) {
        # jolly
    }
    elsif ( 'xml' ne $idx or ! $self->supports('index_xml') ) {
        $self->print_to_server('invalid --index '.$idx,
	                       $Amanda::Script_App::ERROR);
    }
}

=head3 C<check_backup_options>

Check sanity of the (standard) options parsed from the command line
for a C<backup> operation. Override to check any additional properties
for the application.

=cut

sub check_backup_options {
    my ( $self ) = @_;

    $self->check_message_index_options();
    $self->check_level_option();

    if ( $self->{'options'}->{'record'} and ! $self->supports('record') ) {
        $self->print_to_server('not supported --record',
	                       $Amanda::Script_App::ERROR);
    }
}

=head3 C<check_level_option>

Check sanity of C<--level> option(s) parsed from the command line.
In order to work in cases that do or don't support C<multi_estimate>,
this will check either a single value that isn't an array, or every
element of a value that's an array reference.

=cut

sub check_level_option {
    my ( $self ) = @_;

    my $lvls = $self->{'options'}->{'level'};
    return if !defined($lvls);
    $lvls = [ $lvls ] if ref($lvls) ne 'ARRAY';

    for my $lvl (@$lvls) {
        if ( 0 > $lvl  or  $lvl > $self->max_level() ) {
            $self->print_to_server('out of range --level '.$lvl,
	                           $Amanda::Script_App::ERROR);
        }
    }
}

=head3 C<emit_index_entry>

    $self->emit_index_entry($name)

Write I<$name> to the index stream. The caller is responsible to make sure
that I<$name> begins with a C</>, is relative to the I<--device>, and ends
with a C</> if it is a directory. Otherwise it should be the exact name that
the OS would be given to open the file. This method will handle quoting
any special characters in the name as needed within the index file. It will use
the same quoting rules as GNU C<tar> in C<--quoting-style=escape> mode.

=cut

sub emit_index_entry {
    my ( $self, $name ) = @_;
    die 'default emit_index_entry supports only "line"'
    	unless 'line' eq $self->{'options'}->{'index'};
    $self->{'index_out'}->print(blessed($self)->gtar_escape_quote($name)."\n");
}

=head3 C<command_backup>

Performs common housekeeping tasks, calling C<inner_backup> to do the
application's real work. First calls C<check_backup_options> to verify
the invocation, creates the C<index_out> file handle, and calls
C<inner_backup> passing the fd to use for output.

On return from C<inner_backup>, closes the output fd and the index handle,
and writes the C<sendbackup: size> line based on the size in bytes returned
by C<inner_backup> (unless the returned size is negative, in which case no
C<sendbackup> line is written).

=cut

sub command_backup {
    my ( $self ) = @_;
    $self->check_backup_options();

    $self->{'index_out'} = IO::Handle->new_from_fd(4, 'w');

    my $fdout = fileno(STDOUT);
    my $size = $self->inner_backup($fdout);
    POSIX::close($fdout);

    $self->{'index_out'}->close;
    if ($size->bcmp(0) >= 0) {
	my $ksize = $size->copy()->badd(1023)->bdiv(1024);
	if ($ksize->bcmp(32) < 0) {
	    $ksize = 32; # no need to represent 32 as a BigInt....
	} else {
	    $ksize = $ksize->bstr();
	}
	print {$self->{mesgout}} "sendbackup: size $ksize\n";
    }

    # Here's a kludge for you ... the same commit that created the
    # sendbackup_crc feature (a4e01f9) also shifted the responsibility for
    # sending the "end" line, so it will be done for us in sendbackup.c.
    # If using this module in an older Amanda without that feature, it still
    # has to be sent here.

    if ( !defined $Amanda::Feature::fe_sendbackup_crc ) {
        print {$self->{mesgout}} "sendbackup: end\n";
    }
}

=head3 C<inner_backup>

    $size = $self->inner_backup($outfd)

In many cases, the application should only need to override this method to
perform a backup. The backup stream should be written to I<$outfd>, and the
number of bytes written should be returned, as a C<Math::BigInt>.

If not overridden, this default implementation writes nothing and returns zero.

=cut

sub inner_backup {
    my ( $self, $outfd ) = @_;
    return Math::BigInt->bzero();
}

=head3 C<shovel>

    $size = $self->shovel($fdfrom, $fdto)

Shovel all the available bytes from I<$fdfrom> to I<$fdto>, returning the
number of bytes shoveled as a C<Math::BigInt>.

=cut

sub shovel {
    my ( $self, $fdfrom, $fdto ) = @_;
    my $size = Math::BigInt->bzero();
    my $s;
    my $buffer;
    while (($s = POSIX::read($fdfrom, $buffer, 32768)) > 0) {
	Amanda::Util::full_write($fdto, $buffer, $s);
	$size->badd($s);
    }
    return $size;
}

=head2 METHODS FOR THE C<restore> SUBCOMMAND

=head3 C<check_restore_options>

Check sanity of the (standard) options parsed from the command line
for a C<restore> operation. Override to check any additional properties
for the application.

=cut

sub check_restore_options {
    my ( $self ) = @_;

    $self->check_message_index_options();

    $self->check_level_option();

    my $dar = $self->{'options'}->{'opt_dar'}; # {'dar'} is a code ref
    my $rdsf = $self->{'options'}->{'recover_dump_state_file'};

    if ( $dar and ! $self->supports('dar') ) {
        $self->print_to_server('not supported --dar',
	                       $Amanda::Script_App::ERROR);
    }

    if ( $rdsf and ! $self->supports('recover_dump_state_file') ) {
        $self->print_to_server('not supported --recover-dump-state-file',
	                       $Amanda::Script_App::ERROR);
    }

    if ( $dar xor $rdsf ) {
        $self->print_to_server(
	    '--dar=YES and --recover-dump-state-file only make sense together',
	                       $Amanda::Script_App::ERROR);
    }
}

=head3 C<emit_dar_request>

    $self->emit_dar_request($offset, $size)

A C<restore> subcommand that supports Direct Access Recovery can be passed
C<--dar YES> and C<--recover-dump-state-file >I<filename>, read the I<filename>
to recover the backup stream position information saved at backup time, then
look up the names requested for restoration to determine what ranges of the
backup stream will actually be needed to complete the recovery. It calls
C<emit_dar_request> once for each contiguous range needed; I<$offset> and
I<$size> are both in units of bytes.

=cut

sub emit_dar_request {
    my ( $self, $offset, $size ) = @_;
    $self->{'dar_out'}->print('DAR '.$offset->bstr().':'.$size->bstr()."\n");
}

=head3 C<command_restore>

Performs common housekeeping tasks, calling C<inner_restore> to do the
application's real work. First calls C<check_restore_options> to verify
the invocation. If DAR is supported and requested, opens the
recover-state-file for reading and the DAR stream for writing.
Changes directory to C<Amanda::Util::get_original_cwd()>, or to the value of
a C<--directory> property if the application declared such an option and it
was seen on the command line.

Calls C<inner_restore> passing the input stream fileno, the recover-state-file
handle (or C<undef> if DAR was not requested), and the command-line arguments
(indicating objects to be restored); that is, C<inner_restore> has a
variable-length argument list after the first two.

On return from C<inner_restore>, closes the recover-state-file and DAR handles
if used.

=cut

sub command_restore {
    my ( $self ) = @_;
    $self->check_restore_options();

    my $dsf;
    my $rdsf = $self->{'options'}->{'recover-dump-state-file'};
    if ( defined $rdsf ) {
        open $dsf, '<', $rdsf;
	if ( !defined $dsf ) {
	    $self->print_to_server_and_die("Can't open '$rdsf': $!",
				       $Amanda::Script_App::ERROR);
	}
        $self->{'dar_out'} = IO::Handle->new_from_fd(3, 'w');
    }

    chdir(Amanda::Util::get_original_cwd());
    if (defined $self->{directory}) {
        if (!-d $self->{directory}) {
            $self->print_to_server_and_die("Directory $self->{directory}: $!",
                                           $Amanda::Script_App::ERROR);
        }
        if (!-w $self->{directory}) {
            $self->print_to_server_and_die("Directory $self->{directory}: $!",
                                           $Amanda::Script_App::ERROR);
        }
        chdir($self->{directory});
    }

    # XXX at this point, names-to-restore in @ARGV may need some unescaping
    # done. First get signs of life, then test how Amanda is in fact passing
    # them, to determine what is needed.

    $self->inner_restore(fileno(STDIN), $dsf, @ARGV);

    if ( defined $dsf ) {
        close $dsf;
	close $self->{'dar_out'};
    }
}

=head3 C<inner_restore>

    $self->inner_restore($infd, $dsf, $filetorestore...)

Should be overridden to do the actual restoration. Reads stream from I<$infd>,
restores objects represented by the I<$filetorestore> arguments. If the
application supports DAR, should check I<$dsf>: if it is defined, it is a
readable file handle; read the state from it and then call C<emit_dar_request>
to request the needed backup stream regions to recover the wanted objects.

If not overridden, this default implementation reads, and does, nothing.

=cut

sub inner_restore {
    my $self = shift;
    my $fdin = shift;
    my $dsf = shift;
}

=head2 METHODS FOR THE C<index> SUBCOMMAND

=head3 C<check_index_options>

Check sanity of the (standard) options parsed from the command line
for an C<index> operation. Override to check any additional properties
for the application.

=cut

sub check_index_options {
    my ( $self ) = @_;

    $self->check_message_index_options();
    $self->check_level_option();
}

=head3 C<command_index>

Performs common housekeeping tasks, calling C<inner_index> to do the
application's real work. First calls C<check_index_options> to verify
the invocation.

Calls C<inner_index>, which is expected to read from C<STDIN>.

On return from C<inner_index>, closes the index-out stream.

=cut

sub command_index {
    my ( $self ) = @_;
    $self->check_index_options();

    $self->{'index_out'} = IO::Handle->new_from_fd(1, 'w');
    $self->inner_index();

    $self->{'index_out'}->close;
}

=head3 C<inner_index>

    $self->inner_index()

Should be overridden to read from C<STDIN> and call C<emit_index_entry> for
each user object represented.

If not overridden, this default implementation reads everything from C<STDIN>
and emits a single entry for C</>.

=cut

sub inner_index {
    my $self = shift;
    $self->emit_index_entry('/');
    $self->default_validate(); # which happens to read all STDIN and do nothing
}

=head2 METHODS FOR THE C<estimate> SUBCOMMAND

=head3 C<check_estimate_options>

Check sanity of the (standard) options parsed from the command line
for an C<estimate> operation. Override to check any additional properties
for the application.

=cut

sub check_estimate_options {
    my ( $self ) = @_;

    $self->check_message_index_options();
    $self->check_level_option();
}

=head3 C<command_estimate>

Performs common housekeeping tasks, calling C<inner_estimate> to do the
application's real work. First calls C<check_estimate_options> to verify
the invocation.

Calls C<inner_estimate> once (for each level, in case of C<multi_estimate>),
which should return a C<Math::BigInt> size in bytes. Writes each size to the
output stream in the required format, assuming a block size of 1K.

=cut

sub command_estimate {
    my ( $self ) = @_;
    $self->check_estimate_options();

    my $lvls = $self->{'options'}->{'level'};
    if ( !defined $lvls ) {
        $self->print_to_server_and_die("Can't estimate without --level");
    }

    my $isArray = ref($lvls) eq 'ARRAY';
    if ( $isArray xor blessed($self)->supports('multi_estimate') ){
        $self->print_to_server_and_die(
	    "--level usage does not match multi_estimate support");
    }
    $lvls = [ $lvls ] if ! $isArray;

    for my $lvl ( @$lvls ) {
        my $size = $self->inner_estimate($lvl);
	if ( $size->bcmp(0) < 0 ) {
	    print "$lvl -1 -1\n";
	} else {
	    my $ksize = $size->copy()->badd(1023)->bdiv(1024);
            $ksize = ($ksize->bcmp(32) < 0) ? 32 : $ksize->bstr();
	    print "$lvl $ksize 1\n";
	}
    }
}

=head3 C<inner_estimate>

Takes one parameter (the level) and returns a C<Math::BigInt> estimating
the backup size in bytes.

If not overridden, this default implementation does nothing but return zero.
Hey, it's an estimate.

=cut

sub inner_estimate {
    my ($self, $level) = @_;
    return Math::BigInt->bzero();
}

=head2 METHODS FOR THE C<selfcheck> SUBCOMMAND

=head3 C<check_selfcheck_options>

Check sanity of the (standard) options parsed from the command line
for a C<selfcheck> operation. Override to check any additional properties
for the application. If not overridden, this checks the same things as
C<check_backup_options>.

=cut

sub check_selfcheck_options {
    my ( $self ) = @_;

    $self->check_backup_options();
}

=head3 C<command_selfcheck>

If not overridden, this simply checks the options and writes a GOOD message.

=cut

sub command_selfcheck {
    my ( $self ) = @_;
    $self->check_selfcheck_options();
    $self->print_to_server("$self->{name} (non-overridden selfcheck)",
                           $Amanda::Script_App::GOOD);
}

=head2 METHODS FOR THE C<validate> SUBCOMMAND

=head3 C<command_validate>

If not overridden, this simply reads the complete data stream
and does nothing with it. (Which is just what would happen if this sub
were absent and the invocation fell back to C<default_validate>, but
providing this sub allows subclasses to refer to it with SUPER and not
have to think about whether C<command_validate> or C<default_validate>
is the right thing to call.)

=cut

sub command_validate {
    my ( $self ) = @_;
    $self->default_validate();
}

1;