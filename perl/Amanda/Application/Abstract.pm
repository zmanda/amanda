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
use Errno;
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

Any Perl exception thrown from the C<do> is caught. If it is a subclass of
C<Amanda::Application::Exception>, its C<on_uncaught> method is called, which
ordinarily prints an error description to the server and exits with a nonzero
status. If it is any other kind of object, or not an object, it is passed to
C<print_to_server_and_die>, with much the same effect.

=cut

# A superclass uses Amanda::Debug, which, among other things, sets its own
# __WARN__ and __DIE__ handlers. The __DIE__ handler turns an exception into
# a call to Amanda::Debug::critical, but only does that when not in 'eval'
# state, so an exception will work as expected within an eval. Outside of an
# eval, it results in a non-zero exit.

sub run {
    my ( $class ) = @_;
    my $subcommand = shift @ARGV || '(empty)';
    if ( $subcommand !~ /^(?:backup|discover|estimate|index|print|
                             restore|selfcheck|support|validate)/x ) {
	die Amanda::Application::InvocationError->transitionalError(
	    item => 'subcommand', value => $subcommand, problem => 'unknown');
    }

    my %opthash = ();
    my @optspecs = ();

    my $optinit = $class->can('declare_'.$subcommand.'_options');
    unless ( defined $optinit ) {
	die Amanda::Application::ImplementationLimitError->transitionalError(
	    item => 'subcommand', value => $subcommand, problem => 'unsupported'
	);
    }

    $class->$optinit(\%opthash, \@optspecs);

    unless ( GetOptions(\%opthash, @optspecs) ) {
	die Amanda::Application::InvocationError->transitionalError(
	    item => 'options', problem => 'invalid');
    }

    # The calling Amanda code has taken care of unwrapping any quoting/escaping
    # applied to option values for transfer over the network; the options as
    # passed via exec are in their proper, usable form.

    my $app = $class->new(\%opthash);

    Amanda::Debug::debug("Options: " . Data::Dumper->new([$app->{options}])
				     ->Sortkeys(1)->Terse(1)->Useqq(1)->Dump());

    eval {
        $app->do($subcommand);
    };
    if ( my $exc = $@ ) {
	# Handling of exceptions may rely on the proper values of $app->{action}
	# and $app->{mesgout} being set early by do() and remaining set
	# thereafter.
	if ( defined blessed($exc) and
	     $exc->isa('Amanda::Application::Message') ) {
	    $exc->on_uncaught($app);
	}
	else {
	    $app->print_to_server_and_die(
		"unexpected: " . $exc, $Amanda::Script_App::FAILURE);
	}
    }
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
be stored with a different key derived from I<o> instead, which will not collide
with any usable C<GetOptions> option name. Option-validation code should use
C<store_option> to store the final, validated value; it uses the same
convention, so the value will be properly retrieved here.

Design to defer checks, or other operations that may throw exceptions, into
mathods called after C<new>, such as a C<check...> or C<command...> method.
Exceptions thrown from C<new> itself will not be caught.

Will set C<$self-\>{cmd_from_sendbackup}> and C<$self-\>{cmd_to_sendbackup}
(to integer file descriptor numbers) if the corresponding options were passed;
the superclass expects this, and will (in C<do>) set C<$self-\>{cmdin}> and
C<$self-\>{cmdout}> to corresponding Perl filehandles.

=cut

sub new {
    my ( $class, $refopthash ) = @_;
    my %options = ();
    for ( my ($k, $v) ; ($k, $v) = each %{$refopthash} ; ) {
	next if 0 == ord($k);
	if ( "CODE" ne ref $v ) {
	    $options{$k} = $v;
	}
	else {
	    $options{$k} = $refopthash->{"\x00".$k}
	}
    }
    my $self = $class->SUPER::new($options{'config'});
    $self->{'options'} = \%options;

    $self->{'cmd_from_sendbackup'} = $self->{'options'}->{'cmd-from-sendbackup'}
	if exists $self->{'options'}->{'cmd-from-sendbackup'};
    $self->{'cmd_to_sendbackup'} = $self->{'options'}->{'cmd-to-sendbackup'}
	if exists $self->{'options'}->{'cmd-to-sendbackup'};

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
I<%opthash> by calling C<store_option>.
For example, a C<declare_common_options> method that defines a boolean
property I<foo> may contain the snippet:

    $refopthash->{'foo'} = $class->boolean_property_setter($refopthash);

Properties with scalar or hash values can be supported (the
setter sub can tell by the number of parameters passed to it by
C<Getopt::Long>), as well as multiple-valued ones accumulating into an array,
provided an array has been installed with C<store_option> before the options
are parsed.

A property setter calls C<die> with a simple string message, not an exception
object, as it will be caught by C<Getopt::Long> itself and passed to C<warn>,
and cause C<Getopt::Long> to return false when finished.

    store_option(\%opthash, optname, [k,], v)

Store into I<%opthash> a value I<v> of option I<optname>. For an option that
accumulates in a hash (the optspec ends with C<%>), a key I<k> (not undef)
must also be supplied. If I<%opthash> already contains an array at the
corresponding slot, the option will be assumed multivalued, and I<v> will be
appended to the array.

This sub implements the same convention observed by C<new()> for locating the
option value if C<$opthash{$optname}> is a code reference (validation sub).

=cut

sub store_option {
    my ( $class, $refopthash, $optname, $k, $v);
    $class = shift;
    $refopthash = shift;
    $optname = shift;
    $k = shift if 2 == scalar(@_);
    $v = shift;
    my $existing_val = $refopthash->{$optname};
    if ( defined($existing_val) and "CODE" eq ref $existing_val ) {
	$optname = "\x00" . $optname;
	$existing_val = $refopthash->{$optname};
    }
    if ( defined $k ) {
	$refopthash->{$optname}->{$k} = $v;
    }
    elsif ( defined($existing_val) and "ARRAY" eq ref $existing_val ) {
	push @{$refopthash->{$optname}}, $v;
    }
    else {
	$refopthash->{$optname} = $v;
    }
}

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
	$class->store_option($refopthash, $optname, $k, $b);
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
In that case, the code reference should ultimately call C<store_option>
to store the validated value.

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
C<state-stream>, C<timestamp>, C<cmd-to-sendbackup>, C<cmd-from-sendbackup>, and
C<server-backup-result> if supported.

=cut

sub declare_backup_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, "target|directory=s";
    push @$refoptspecs, (
        'message=s', 'index=s', 'level=i', 'record', 'state-stream=i' );
    push @$refoptspecs, "timestamp=s" if $class->supports('timestamp');

    push @$refoptspecs, "cmd-to-sendbackup=i", "cmd-from-sendbackup=i"
	if $class->supports('cmd_stream');
    push @$refoptspecs, "server-backup-result"
	if $class->supports('want_server_backup_result');
}

=head2 C<declare_restore_options>

Declare options for the C<restore> subcommand. Unless overridden, this simply
declares the C<common> ones plus C<message>, C<index>, C<level>, C<dar>, and
C<recover-dump-state-file>.

=cut

sub declare_restore_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, "target|directory=s";
    push @$refoptspecs, (
        'message=s', 'index=s', 'level=i',
	'dar=s', 'recover-dump-state-file=s' );
    $refopthash->{'dar'} = sub {
        my ( $optname, $optval ) = @_;
	if ( $optval eq 'YES' ) {
	    $class->store_option($refopthash, $optname, 1);
	}
	elsif ( $optval eq 'NO' ) {
	    $class->store_option($refopthash, $optname, 0);
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
    push @$refoptspecs, "target|directory=s";
    push @$refoptspecs, (
        'message=s', 'level=i'.($class->supports('multi_estimate') ? '@' : '' )
    );
    push @$refoptspecs, "calcsize" if $class->supports('calcsize');
    push @$refoptspecs, "timestamp=s" if $class->supports('timestamp');
}

=head2 C<declare_selfcheck_options>

Declare options for the C<selfcheck> subcommand. Unless overridden, this simply
declares the C<common> ones plus C<message>, C<index>, C<level>, and C<record>.

=cut

sub declare_selfcheck_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    $class->declare_common_options($refopthash, $refoptspecs);
    push @$refoptspecs, "target|directory=s";
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

=head2 C<supports_cmd_stream>

Can command pipes to and from the parent (sendbackup) be accepted?
Default if not overridden is C<1>.

=cut

sub supports_cmd_stream { my ( $class ) = @_; return 1; }

=head2 C<supports_want_server_backup_result>

Can the application use a final backup result from the parent (sendbackup)?
Default if not overridden is C<1>.

=cut

sub supports_want_server_backup_result { my ( $class ) = @_; return 1; }

=head2 C<supports>

    $class->supports(name)

Returns C<false> if there is no C<supports_>I<name>C<()> class method,
otherwise calls it and returns its result.

=cut

sub supports {
    my ( $class, $supname ) = @_;
    my $s = $class->can("supports_".$supname);
    return (defined $s) ? $class->$s() : 0;
}

=head2 C<max_level>

Returns the maximum C<level> supported, Default if not overridden is zero,
indicating no support for incremental backup. Where incremental backup is
supported, this should return a fixed, maximum level supported (regardless
of the current state of prior levels backed up, which will limit the level
that can be requested in practice). May return a number or the string "DEFAULT",
which will be replaced with the largest value Amanda supports.

=cut

sub max_level { my ( $class ) = @_; return 0; }

# INSTANCE METHODS SUPPORTING C<support> SUBCOMMAND

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
    die Amanda::Application::ImplementationError->transitionalError(
        problem => "unusable", item => "recover_mode", value => $rm
    ) if defined $rm and $rm ne "SMB";
    return $rm;
};

# data_path (class method) should return list: "AMANDA", "DIRECTTCP", or both
my $checked_data_path = sub {
    my ( $self ) = @_;
    my @dp = blessed($self)->data_path();
    my $dpl = scalar @dp;
    die Amanda::Application::ImplementationError->transitionalError(
        problem => "unusable", item => "data_path", value => Dumper(\@dp)
    ) if $dpl lt 1 or 0 lt grep(!/^(?:AMANDA|DIRECTTCP)$/, @dp);
    return @dp;
};

# recover_path (class method) should return either "CWD" or "REMOTE"
my $checked_recover_path = sub {
    my ( $self ) = @_;
    my $rp = blessed($self)->recover_path();
    die Amanda::Application::ImplementationError->transitionalError(
        problem => "unusable", item => "recover_path", value => $rp
    ) unless defined $rp and $rp =~ /^(?:CWD|REMOTE)$/;
    return $rp;
};

# max_level (class method) should return 0..$amanda_max or 'DEFAULT'
my $checked_max_level = sub {
    my ( $self ) = @_;
    my $amanda_max = 399;
    my $mxl = blessed($self)->max_level();
    $mxl = $amanda_max if 'DEFAULT' eq $mxl;
    die Amanda::Application::ImplementationError->transitionalError(
        problem => "unusable",  item => "max_level", value => $mxl
    ) unless defined $mxl and $mxl =~ /^\d{1,3}$/ and 0 + $mxl <= $amanda_max;
    return 0 + $mxl;
};

my $checked_want_server_backup_result = sub {
    my ( $self ) = @_;
    my $has_cmd_stream = blessed($self)->supports('cmd_stream');
    my $wants_sbr = blessed($self)->supports('want_server_backup_result');
    die Amanda::Application::ImplementationError->transitionalError(
	item => "want_server_backup_result",
        problem => "cannot be supported unless cmd_stream is")
	if $wants_sbr and not $has_cmd_stream;
    return $wants_sbr;
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

An application that uses local state should read it into
C<$self->{'localstate'}>, as C<command_backup> will call C<write_local_state>
on that member automatically if the caller has asked to record state.

=cut

sub read_local_state {
    my ( $self, $optspecs ) = @_;
    my ( $dirpart, $filepart ) = $self->local_state_path();
    my $fn = File::Spec->catfile($dirpart, $filepart);
    my $ret = open my $fh, '<', $fn;
    if ( ! $ret ) {
	if ( $!{ENOENT} ) {
	    my %empty = ( 'maxlevel' => 0 );
	    return \%empty;
	}
	die Amanda::Application::EnvironmentError->transitionalError(
	    item => 'local state file', value => $fn, problem => 'read',
	    errno => $!);
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
    die Amanda::Application::EnvironmentError->transitionalError(
	item => 'local state file', value => $fn, problem => 'malformed')
	unless $ret and (0 == scalar(@$remain) or $remain->[0] =~ m/^--/);
    my $lev = $opthash->{'level'};
    $maxlevel = $lev if $lev > $maxlevel;
    $result{$lev} = $opthash;
    while ( 0 < scalar @$remain ) {
	$opthash = {};
	$ret = Getopt::Long::GetOptionsFromArray($remain, $opthash, @$optspecs);
	die Amanda::Application::EnvironmentError->transitionalError(
	    item => 'local state file', value => $fn, problem => 'malformed')
	    unless $ret;
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

This is normally called by C<command_backup> as part of successful completion.
For unsuccessful completion, C<repair_local_state> should be called instead.

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
    my $fn = File::Spec->catfile($dirpart, $filepart);
    Amanda::Util::safe_overwrite_file($fn, $state)
    or die Amanda::Application::EnvironmentError->transitionalError(
	errno => $!, item => 'local state file', value => $fn,
	problem => 'overwrite');
}

=head2 C<repair_local_state>

    $self->repair_local_state()

If an application might allocate resources during backup that would normally
be referred to as part of the saved local state, but an unsuccessful exit
(that does not call C<write_local_state>) would leave those resources leaked
(not referred to by the state, and not reclaimed), then the application should
override this method to reclaim them.

If not overridden, this method does nothing.

=cut

sub repair_local_state {
    my ( $self ) = @_;
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

=head1 INSTANCE METHOD SUPPORTING POST-PARSE OPTION/PROPERTY CHECKS

Option/property checks can happen two ways. The C<declare_..._options>
methods can set a coarse limit on what options are accepted, and
C<..._property_setter> methods can further restrict just what values
are accepted for a particular option. Those will not catch other kinds
of errors, such as combinations of options that make no sense together,
missing ones that are needed, etc. Those higher-level checks can be made
in C<check_..._options> instance methods, after the options have been
parsed and the app has been instantiated.

=head2 C<check>

    $self->check(boolean-expr, message)

If I<boolean-expr> is false, pass I<message> to C<print_to_server> with a
severity of C<ERROR>, and remember that a check failed.

    $self->check()

If any previous check (invocation of this method with arguments) failed,
throw an exception. By dividing the labor this way, several checks can be
performed, and all detected errors reported to the server, before the final
C<check()> that terminates execution by throwing the exception.

=cut

sub check {
    my ( $self, $bval, $message ) = @_;
    unless ( 1 == scalar(@_) ) {
	$self->{'checkstate'} = 1 unless exists $self->{'checkstate'};
	return if $bval;
	$self->{'checkstate'} = 0;
	$self->print_to_server($message, $Amanda::Script_App::ERROR);
	return;
    }
    unless ( exists $self->{'checkstate'} ) {
	die Amanda::Application::ImplementationError->transitionalError(
	    item => 'check', problem => 'called before anything checked');
    }
    unless ( $self->{'checkstate'} ) {
	die Amanda::Application::InvocationError->transitionalError(
	    item => 'check', value => $self->{'action'},
	    problem => 'error(s) detected');
    }
}

=head1 INSTANCE METHODS USABLE IN SUBCOMMANDS

=head2 Checked conversions

=head3 C<int2big>

    $self->int2big(n)

Return a C<Math::BigInt> version of I<n>, throwing an exception if it is already
too late (something that could happen in a perl with 32-bit ints if it has
already widened I<n> to a float and lost precision).

=cut

sub int2big {
    my ( $self, $n ) = @_;
    die Amanda::Application::PrecisionLossError->transitionalError(
	item => 'numeric value', value => $n)
	if $n - 1 == $n or $n == $n + 1;
    my $big = Math::BigInt->new($n);
    die Amanda::Application::PrecisionLossError->transitionalError(
	item => 'numeric value', value => $n)
	unless $big->is_int();
    return $big;
}

=head3 C<big2int>

    $self->big2int(n)

Return the native perl number corresponding to the C<Math::BigInt> I<n>,
throwing an exception if I<n> gets widened to something (a perl float, say)
that does not retain precision to the units place.

=cut

sub big2int {
    my ( $self, $big ) = @_;
    my $n = $big->numify();
    die Amanda::Application::PrecisionLossError->transitionalError(
	item => 'numeric value', value => $big->bstr())
	if $n - 1 == $n or $n == $n + 1;
    return $n;
}

=head2 Determining target of operation

=head3 C<target>

    $self->target([default])

Return the target of the operation, to be interpreted as a directory name
by applications that act on directory trees, or as a file name by an application
that acts on a single object.

The value returned will be that of the TARGET property, if that has been given.
Otherwise, for actions other than restore, it will be taken from the DEVICE
property, if that has been given. If not determined by either of those rules,
it will be the value of I<default> if provided; otherwise an exception will be
thrown. Pass C<undef> as I<default> in order to simply return C<undef> rather
than throwing the exception.

This provides the usual behavior for backup, using DEVICE unless explicitly
overridden by TARGET. When restoring, if C<undef> is returned, restoration
should be into the directory named by C<Amanda::Util::get_original_cwd>; when
not overridden, C<command_restore> ensures the current directory is that one.
For an application that acts on a directory tree, C<chdir_to_target> should be
called to override that location in case a TARGET property has been given.
An application that acts on a single object should supply a default file name
(not an absolute path). It will be returned if there is no explicit TARGET,
which should leave the application creating a file by that default name in
the current directory.

=cut

sub target {
    my ( $self, $default ) = @_;

    if ( exists $self->{'options'}->{'target'} ) {
	return $self->{'options'}->{'target'};
    }
    if ( 'restore' ne $self->{'action'} and
	 exists $self->{'options'}->{'device'} ) {
        return $self->{'options'}->{'device'};
    }
    return $default if 1 < scalar(@_);

    die Amanda::Application::InvocationError->transitionalError(
	item => 'target', problem => 'must be specified; there is no default');
}

=head3 C<chdir_to_target()

Change directory to the value returned by C<target>, if it is not C<undef>.

If C<target> returns C<undef>, no directory change is done.

Throw an exception if the directory change fails. This assumes that the target
should be interpreted as a directory, so only applications that act on directory
trees should call this method, not an application that acts on a single object.

=cut

sub chdir_to_target {
    my ( $self ) = @_;

    my $tg = $self->target(undef);

    return unless defined $tg;

    chdir $tg
    or die Amanda::Application::EnvironmentError->transitionalError(
	item => 'target', value => $tg, errno => $!);
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

    if ( defined $Amanda::Feature::fe_amidxtaped_dar ) {
	$self->$say_supports("RECOVER-DUMP-STATE-FILE",
			     "recover_dump_state_file");
	$self->$say_supports(                    "DAR", "dar");
	$self->$say_supports(           "STATE-STREAM", "state_stream");
    }

    if ( defined $Amanda::Feature::fe_req_options_timestamp ) {
        $self->$say_supports("TIMESTAMP", "timestamp");
    }

    if ( defined $Amanda::Feature::fe_sendbackup_stream_cmd ) {
        $self->$say_supports("CMD-STREAM", "cmd_stream");
    }

    if ( defined $Amanda::Feature::fe_sendbackup_stream_cmd_get_dumper_result ){
        print "WANT-SERVER-BACKUP-RESULT ".
	    ($self->$checked_want_server_backup_result()? 'YES': 'NO')."\n";
    }
}

=head2 METHODS FOR THE C<backup> SUBCOMMAND

=head3 C<check_message_index_options>

Check sanity of the (standard) options parsed from the command line
for C<--message> and C<--index>.

=cut

sub check_message_index_options {
    my ( $self ) = @_;

    my $msg = $self->{'options'}->{'message'};
    if ( ! defined $msg and blessed($self)->supports('message_line') ) {
        $self->{'options'}->{'message'} = 'line';
    }
    elsif ( 'line' eq $msg and blessed($self)->supports('message_line') ) {
        # jolly
    }
    elsif ( 'xml' ne $msg or ! blessed($self)->supports('message_xml') ) {
        $self->check(0, 'invalid --message '.$msg,);
    }

    my $idx = $self->{'options'}->{'index'};
    if ( ! defined $idx and blessed($self)->supports('index_line') ) {
        $self->{'options'}->{'index'} = 'line';
    }
    elsif ( 'line' eq $idx and blessed($self)->supports('index_line') ) {
        # jolly
    }
    elsif ( 'xml' ne $idx or ! blessed($self)->supports('index_xml') ) {
        $self->check(0, 'invalid --index '.$idx);
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

    $self->check(
	! $self->{'options'}->{'record'} or blessed($self)->supports('record'),
	'not supported --record');

    $self->check(
	! $self->{'options'}->{'server-backup-result'} or
	    defined $self->{cmd_to_sendbackup} and
	    defined $self->{cmd_from_sendbackup},
	'--server-backup-result without --cmd-to/from-sendbackup');
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
	$self->check(0 <= $lvl  and  $lvl <= $self->$checked_max_level(),
		     'out of range --level '.$lvl);
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
    die Amanda::Application::ImplementationLimitError->transitionalError(
        item => 'emit_index_entry', value => $self->{'options'}->{'index'},
	problem => 'only "line" supported')
    	unless 'line' eq $self->{'options'}->{'index'};
    $self->{'index_out'}->print(blessed($self)->gtar_escape_quote($name)."\n");
}

=head3 C<backup_succeeded>

    $self->backup_succeeded()

If the parent has not passed C<--server-backup-result>, return true as an
optimistic assumption; otherwise, wait for the parent to report the final
success of storing the backup, and return true for a result of success or false
for a result of failure, or throw an exception for any other response.

=cut

sub backup_succeeded {
    my ( $self ) = @_;

    return 1 unless $self->{'options'}->{'server-backup-result'};

    my $report = $self->{'cmdin'}->getline();
    chomp $report;
    return 1 if 'SUCCESS' eq $report;
    return 0 if 'FAILED' eq $report;
    die Amanda::Application::EnvironmentError->transitionalError(
	item => 'backup result from server', value => $report,
	problem => 'neither SUCCESS nor FAILED');
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

If C<--record> is supported and requested, and C<$self->{'localstate'}> is
defined, calls C<write_local_state($self->{'localstate'})>.

=cut

sub command_backup {
    my ( $self ) = @_;
    $self->check_backup_options();
    $self->check();

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

    if ( $self->{'options'}->{'record'} and defined $self->{'localstate'} ) {
	if ( $self->backup_succeeded() ) {
	    $self->write_local_state($self->{'localstate'});
	} else {
	    $self->repair_local_state(); # app may need to roll something back
	}
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

    $size = $self->inner_backup($fdout)

In many cases, the application should only need to override this method to
perform a backup. The backup stream should be written to I<$fdout>, and the
number of bytes written should be returned, as a C<Math::BigInt>.

If not overridden, this default implementation writes nothing and returns zero.

=cut

sub inner_backup {
    my ( $self, $fdout ) = @_;
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
    my $rd;
    my $wn;
    my $buffer;
    while (($rd = POSIX::read($fdfrom, $buffer, 32768)) > 0) {
	$wn = Amanda::Util::full_write($fdto, $buffer, $rd);
	die Amanda::Application::EnvironmentError->transitionalError(
	    errno => $!, item => 'fd', value => $fdto, problem => 'write')
	    unless defined $wn;
	$size->badd($wn);
    }
    die Amanda::Application::EnvironmentError->transitionalError(
	errno => $!, item => 'fd', value => $fdfrom, problem => 'read')
	unless defined $rd;
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

    my $dar = $self->{'options'}->{'dar'};
    my $rdsf = $self->{'options'}->{'recover-dump-state-file'};

    $self->check( ! $dar or blessed($self)->supports('dar'),
		  'not supported --dar');

    $self->check(! $rdsf or blessed($self)->supports('recover_dump_state_file'),
		 'not supported --recover-dump-state-file');

    $self->check( !( $dar xor $rdsf ),
	    '--dar=YES and --recover-dump-state-file only make sense together');
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
    $self->check();

    my $dsf;
    my $rdsf = $self->{'options'}->{'recover-dump-state-file'};
    if ( defined $rdsf ) {
	open $dsf, '<', $rdsf
	or die Amanda::Application::EnvironmentError->transitionalError(
	    item => 'dump state file', value => $rdsf, errno => $!);
        $self->{'dar_out'} = IO::Handle->new_from_fd(3, 'w');
    }

    chdir(Amanda::Util::get_original_cwd());

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

    $self->inner_restore($fdin, $dsf, $filetorestore...)

Should be overridden to do the actual restoration. Reads stream from I<$fdin>,
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
    $self->check();

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

    $self->check(exists $self->{'options'}->{'level'},
		 "Can't estimate without --level");
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
    $self->check();

    my $lvls = $self->{'options'}->{'level'}; # existence already checked
    my $isArray = ref($lvls) eq 'ARRAY';

    # This next is a Should Never Happen, assuming declare_estimate_options has
    # done its job; hence the ImplementationError rather than simply check().
    if ( $isArray xor blessed($self)->supports('multi_estimate') ){
        die Amanda::Application::ImplementationError->transitionalError(
	    item => 'option parsing', value => 'level',
	    problem => 'mismatch with multi_estimate support');
    }
    $lvls = [ $lvls ] if ! $isArray;

    my $anyLevelSucceeded = 0;
    my @failReasons;

    for my $lvl ( @$lvls ) {
	my ( $size, $validsize );
	eval {
	    $size = $self->inner_estimate($lvl);
	    $validsize = ($size->isa('Math::BigInt') and $size->bcmp(0) >= 0);
	};
	if ( $@ or ! $validsize ) {
	    push @failReasons, $@;
	    if ( Amanda::Application::DiscontiguousLevelError->captures($@) ) {
		print "$lvl -2 -2\n";
	    }
	    else {
		print "$lvl -1 -1\n";
	    }
	} else {
	    $anyLevelSucceeded = 1;
	    my $ksize = $size->copy()->badd(1023)->bdiv(1024);
            $ksize = ($ksize->bcmp(32) < 0) ? 32 : $ksize->bstr();
	    print "$lvl $ksize 1\n";
	}
    }

    die Amanda::Application::MultipleError->transitionalError(
	exceptions => \@failReasons) unless ( $anyLevelSucceeded );
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
    $self->check();
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

package Amanda::Application::Message;
use base qw(Amanda::Message);
use Scalar::Util qw{blessed};

# CLASS method to determine if a given thing is an exception that's an
# instance of this class or a subclass (sort of isa in reverse).
sub captures {
    my ( $class, $thing ) = @_;
    return ( blessed($thing) and $thing->isa($class) );
}

# Private sub to relieve application authors of the need to mention __FILE__
# and __LINE__ everywhere an exception might be thrown. Return a usually-best
# file and line by following the call stack outward until (a) out of any code
# from Amanda::Message or subclasses and (b) out of this package itself (unless
# (c) that unwinds all the way out to Script_App, in which case use the
# innermost file/line found in this package). It is still possible to pass
# source_filename and source_line explicitly to the constructor at particular
# call sites.
my $callerfill = sub {
    my ( $package, $sourcefile, $sourceline );
    my ( $myfile, $myline );
    # can start at depth 1; zero is just the call to this private routine
    for ( my $depth = 1; my @frameinfo = caller($depth); ++ $depth ) {
        ( $package, $sourcefile, $sourceline ) = @frameinfo;
	next if $package->isa('Amanda::Message');
	last unless 'Amanda::Application::Abstract' eq $package;
	( $myfile, $myline ) = ( $sourcefile, $sourceline )
	    unless defined $myfile or defined $myline;
    }
    return ($myfile, $myline) if 'Amanda::Script_App' eq $package
	and defined $myfile and defined $myline;
    return ($sourcefile, $sourceline);
};

sub new {
    my ( $class, %params ) = @_;
    unless (defined $params{'source_filename'}&&defined $params{'source_line'}){
        ($params{'source_filename'},$params{'source_line'}) = $callerfill->();
    }
    return $class->SUPER::new(%params);
}

# For development transition to using ::Message but before every message has
# a code assigned; this allows code to be omitted, and uses 0 to indicate a
# generic 'good' message. It will also default severity to INFO.
sub transitionalGood {
    my ( $class, %params ) = @_;
    $params{'code'} = 0 unless exists $params{'code'};
    $params{'severity'} = $Amanda::Message::INFO
        unless exists $params{'severity'};
    return $class->new(%params);
}

# For development transition to using ::Message but before every message has
# a code assigned; this allows code to be omitted, and uses 1 to indicate a
# generic 'error' message. The severity, as usual, defaults to CRITICAL.
sub transitionalError {
    my ( $class, %params ) = @_;
    $params{'code'} = 1 unless exists $params{'code'};
    return $class->new(%params);
}

# Make this do something useful, even when not overridden.
sub local_full_message { my ( $self ) = @_; return $self->local_message(); }

sub on_uncaught {
    my ( $self, $app ) = @_;
    $app->print_to_server_and_die($self . '', $Amanda::Script_App::FAILURE);
}

# An exception indicating the application has not been properly invoked.
package Amanda::Application::InvocationError;
use base 'Amanda::Application::Message';
sub local_message {
    my ( $self ) = @_;
    my $lm = 'usage error: ' . $self->{'item'};
    $lm .= ' (' . $self->{'value'} . ')' if defined $self->{'value'};
    $lm .= ': ' . $self->{'problem'};
    return $lm;
}

# An exception indicating some limit of the implementation has been hit (this
# includes where functionality that should be implemented simply isn't yet).
package Amanda::Application::ImplementationLimitError;
use base 'Amanda::Application::Message';
sub local_message {
    my ( $self ) = @_;
    my $lm = 'implementation limit: ' . $self->{'item'};
    $lm .= ' (' . $self->{'value'} . ')' if defined $self->{'value'};
    $lm .= ': ' . $self->{'problem'};
    return $lm;
}

# An exception indicating something is probably wrong in the implementation
# itself; an assertion/should-not-happen kind of thing.
package Amanda::Application::ImplementationError;
use base 'Amanda::Application::Message';
sub local_message {
    my ( $self ) = @_;
    my $lm = 'should not happen: ' . $self->{'item'};
    $lm .= ' (' . $self->{'value'} . ')' if defined $self->{'value'};
    $lm .= ': ' . $self->{'problem'};
    return $lm;
}

# An exception that reflects some condition in the environment where the
# application runs: missing files, I/O errors, the usual stuff that happens.
package Amanda::Application::EnvironmentError;
use base 'Amanda::Application::Message';
sub local_message {
    my ( $self ) = @_;
    my $lm = $self->{'item'};
    $lm .= ' (' . $self->{'value'} . ')' if defined $self->{'value'};
    $lm .= ': ' . $self->{'problem'} if defined $self->{'problem'};
    $lm .= ': ' . $self->{'errnostr'} if defined $self->{'errnostr'};
    return $lm;
}

# An exception reflecting a loss of numeric precision where that isn't ok.
package Amanda::Application::PrecisionLossError;
use base 'Amanda::Application::ImplementationLimitError';
sub new {
    my ( $class, %params ) = @_;
    $params{'problem'} = 'Loss of numeric precision'
	unless exists $params{'problem'};
    $class->SUPER::new(%params);
}

# An exception reflecting the status of a called process.
package Amanda::Application::CalledProcessError;
use base 'Amanda::Application::EnvironmentError';
sub new {
    my ( $class, %params ) = @_;
    $params{'item'} = 'External process'
	unless exists $params{'item'};
    if ( exists $params{'cmd'} and not exists $params{'value'} ) {
	$params{'value'} = Data::Dumper->new([$params{'cmd'}])->
	    Terse(1)->Useqq(1)->Indent(0)->Dump();
    }
    $class->SUPER::new(%params);
}
sub local_message {
    my ( $self ) = @_;
    my $lm = $self->SUPER::local_message();
    $lm .= ': exit status ' . $self->{'returncode'}
	if defined $self->{'returncode'};
    return $lm;
}

# An exception refusing a requested backup level because the prior level is
# not recorded.
package Amanda::Application::DiscontiguousLevelError;
use base 'Amanda::Application::EnvironmentError';
sub new {
    my ( $class, %params ) = @_;
    $params{'item'} = 'Requested level'
	unless exists $params{'item'};
    $params{'problem'} = 'Prior level not recorded';
    $class->SUPER::new(%params);
}

# An exception bundling one or more other exceptions as an array {'exceptions'}.
package Amanda::Application::MultipleError;
use base 'Amanda::Application::Message';
sub local_message {
    my ( $self ) = @_;
    return 'Collected errors: ' . scalar(@{$self->{'exceptions'}});
}
sub on_uncaught {
    my ( $self, $app ) = @_;
    for my $exc ( @{$self->{'exceptions'}} ) {
	$app->print_to_server($exc . '', $Amanda::Script_App::ERROR);
    }
    $app->print_to_server_and_die($self . '', $Amanda::Script_App::FAILURE);
}

# An exception indicating a dump should be retried.
package Amanda::Application::RetryDumpError;
use base 'Amanda::Application::EnvironmentError';
use Amanda::Feature;
use Amanda::Util;
sub local_message {
    my ( $self ) = @_;
    my $lm = 'Should retry';
    $lm .= ' in ' . (0 + $self->{delay}) . ' seconds' if exists $self->{delay};
    $lm .= ' at level ' . (0 + $self->{level}) if exists $self->{level};
    $lm .= ': ' . $self->{problem} if exists $self->{problem};
    return $lm;
}
sub on_uncaught {
    my ( $self, $app ) = @_;
    if ( defined $Amanda::Feature::fe_sendbackup_retry ) {
	if ( 'backup' eq $app->{'action'} ) {
	    my $pm = 'sendbackup: retry';
	    $pm .= ' delay ' . (0 + $self->{delay}) if exists $self->{delay};
	    $pm .= ' level ' . (0 + $self->{level}) if exists $self->{level};
	    $pm .= ' message ' . Amanda::Util::quote_string($self->{problem})
		if exists $self->{problem};
	    print {$app->{mesgout}} $pm . "\n";
	    return;
	}
    }
    # if action isn't 'backup' or feature isn't supported, fall back and behave
    # as an ordinary error.
    $self->SUPER::on_uncaught($app);
}

1;
