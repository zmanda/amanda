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

Amanda::Script::Abstract - A base class for Amanda scripts

=head1 SYNOPSIS

    package amMyScript;
    use base qw(Amanda::Script::Abstract)

    package main;
    amMyScript::->run();

=head1 DESCRIPTION

C<Amanda::Script::Abstract> handles much of the common housekeeping
needed to implement Amanda's
L<Script API|http://wiki.zmanda.com/index.php/Script_API>, so that
practical applications can be implemented in few lines of code by overriding
only a few necessary methods.

C<Amanda::Script::Abstract> is itself a subclass of
C<Amanda::Script>, but the latter cannot be instantiated until after
the command line has been parsed (at least the I<execute-where> argument and
C<config> options need to be
passed to its constructor). Therefore, C<Amanda::Script::Abstract>
supplies I<class> methods for the preliminary work of declaring the supported
options, parsing the command line, and finally calling C<new> to get an instance
of the class. Then the actual operations of the script are carried out by
instance methods, as you would expect.

In perl, class methods are inheritable and overridable just as instance methods
are, and the syntax C<$class-E<gt>SUPER::method(...)> works, analogously to its
familiar use in an instance method. So, the pre-instantiation behavior of a
script (declaring command options, etc.) can be tailored by simply
overriding the necessary class methods, just as its later operations can be
supplied by overriding instance methods.

=cut

package Amanda::Script::Abstract;
use base qw(Amanda::Script);

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

    scriptclass::->run()

Run the script: grab the I<execute-where> from C<ARGV[0]>, be sure it is a known
one, set up for option parsing, parse the options, construct an instance
passing all the supplied options (which will include the config name, needed
by the C<Amanda::Script> constructor). Call C<check_properties> (except when
C<execute_where> is C<support>, for which properties are not passed), and
finally C<do()> the I<execute-where>, where C<do> is inherited from
C<Amanda::Script_App> by way of C<Amanda::Script>.

=cut

sub run {
    my ( $class ) = @_;
    my $execute_where = shift @ARGV || '(empty)';

    my %opthash = ();
    my @optspecs = ();

    $class->declare_options(\%opthash, \@optspecs);

    unless ( GetOptions(\%opthash, @optspecs) ) {
	die Amanda::Script::InvocationError->transitionalError(
	    item => 'options', problem => 'invalid');
    }

    my $script = $class->new($execute_where, \%opthash);

    Amanda::Debug::debug("Options: " . Data::Dumper->new([$script->{options}])
				     ->Sortkeys(1)->Terse(1)->Useqq(1)->Dump());

    $script->check_properties() unless $execute_where =~ m/^support$/i;

    eval {
	$script->do($execute_where); # do() is case-insensitive, you see.
    };
    if ( my $exc = $@ ) {
	# Handling of exceptions may rely on the proper values of $app->{action}
	# and $app->{mesgout} being set early by do() and remaining set
	# thereafter. (Those things do happen for scripts too.)
	if ( defined blessed($exc) and
	     $exc->isa('Amanda::Script::Message') ) {
	    $exc->on_uncaught($script);
	}
	else {
	    $script->print_to_server_and_die(
		"unexpected: " . $exc, $Amanda::Script_App::ERROR);
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

For a I<POST-*> script, if the option C<--success> was passed,
C<$self-\>{'succeeded'}> is set to 1, or to 0 if C<--failed> was passed.
It is not defined under other conditions.

=cut

sub new {
    my ( $class, $execute_where, $refopthash ) = @_;
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
    my $self = $class->SUPER::new($execute_where, $options{'config'});
    $self->{'options'} = \%options;

    my $opt_success = exists $self->{'options'}->{'success'};
    my $opt_failure = exists $self->{'options'}->{'failed'};

    if ( $opt_success ) {
	die Amanda::Script::InvocationError->transitionalError(item => 'option',
	    value => 'success', problem => 'passed to a non-POST-* script')
	    unless $execute_where =~ m/^post-/i;
	die Amanda::Script::InvocationError->transitionalError(item => 'option',
	    value => 'failed', problem => 'combined with --success')
	    if $opt_failure;
	$self->{'succeeded'} = 1;
    }
    elsif ( $opt_failure ) {
	die Amanda::Script::InvocationError->transitionalError(item => 'option',
	    value => 'failed', problem => 'passed to a non-POST-* script')
	    unless $execute_where =~ m/^post-/i;
	$self->{'succeeded'} = 0;
    }

    return $self;
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

=head1 INSTANCE METHODS FOR VALIDATING OPTION/PROPERTY VALUES

=head2 C<check_properties>

Called for every script stage except C<support> (for which properties are
not passed). Can do any general checking of properties, such as that any
required ones are present. If not overridden, this version does nothing,
successfully.

=cut

sub check_properties {
    my ( $self ) = @_;
}

=head1 CLASS METHOD FOR DECLARING ALLOWED OPTIONS / PROPERTIES

=head2 C<declare_options>

    declare_options(\%opthash, \@optspecs)

This is a class method that is passed two references: a hash reference
I<\%opthash> and a list reference I<\@optspecs>. It should push onto
I<\@optspecs> the declarations of whatever options are valid for the
script, using the syntax described in L<Getopt::Long>.
It does not need to touch I<\%opthash>, but may store a code reference at the
name of any option, to apply stricter validation when the option is parsed.
In that case, the code reference should ultimately call C<store_option>
to store the validated value.

=cut

sub declare_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    push @$refoptspecs,
        ( 'config=s', 'host=s', 'disk=s', 'device=s', 'level=i@',
	  'execute-where=s', 'timestamp=s', 'success', 'failed' );
}

=head1 INSTANCE METHODS USABLE IN SUBCOMMANDS

=head2 Setting application properties

=head3 C<setproperty>

    $self->setproperty(name, value...)

Set a property that will be visible to the application. The script must be
running on the client, for PRE-DLE-SELFCHECK, PRE-DLE-ESTIMATE, or
PRE-DLE-BACKUP, and the property will be visible during the corresponding
subcommand of the application. Properties set by scripts are not seen by
other scripts, but properties set by multiple PRE-DLE-* scripts are accumulated
and passed to the application together. A script may assign multiple values
to a property, either in multiple calls to C<setproperty>, or as multiple
I<value> arguments in a single call. Values set in multiple scripts for the
same property are also combined and seen by the application as a multiple-valued
property. The application cannot distinguish this case from a single script
giving the property multiple values.

No property value to be set in this way may contain a newline; nor may any
property value contain a NUL byte.

=cut

sub setproperty {
    my ( $self, $name, @values ) = @_;

    die Amanda::Script::ImplementationError->transitionalError(
	item => 'setproperty', value => $name,
	problem => 'invalid property name')
	unless $name =~ /^\S+$/;

    for my $val ( @values ) {
	die Amanda::Script::ImplementationError->transitionalError(
	    item => 'setproperty', value => $name,
	    problem => 'invalid property value')
	    unless $val =~ /^[^\n\0]*$/;

	print "PROPERTY $name $val\n";
    }
}

=head2 Checked conversions

=head3 C<int2big>

    $self->int2big(n)

Return a C<Math::BigInt> version of I<n>, throwing an exception if it is already
too late (something that could happen in a perl with 32-bit ints if it has
already widened I<n> to a float and lost precision).

=cut

sub int2big {
    my ( $self, $n ) = @_;
    die Amanda::Script::PrecisionLossError->transitionalError(
	item => 'numeric value', value => $n)
	if $n - 1 == $n or $n == $n + 1;
    my $big = Math::BigInt->new($n);
    die Amanda::Script::PrecisionLossError->transitionalError(
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
    die Amanda::Script::PrecisionLossError->transitionalError(
	item => 'numeric value', value => $big->bstr())
	if $n - 1 == $n or $n == $n + 1;
    return $n;
}

=head1 INSTANCE METHODS IMPLEMENTING SUBCOMMANDS

=cut

#
# Private instance method to produce a support-line for a boolean
# capability:  $self->say_supports($confstring, $supname) where $confstring
# is the text to output on fd1, followed by a space and YES or NO and a
# newline, as determined by calling $class->supports($supname). For now, this
# stub simply assumes YES for everything; if the support situation for scripts
# grows more interesting in the future, this should be expanded to behave as
# the Amanda::Application::Abstract version does: unquote the test expr. :)
#

my $say_supports = sub {
    my ( $self, $confstring, $supname ) = @_;
    my $yn = 'blessed($self)->supports($supname)' ? "YES" : "NO";
    print $confstring . " " . $yn . "\n";
};

=head2 C<command_support>

For now, there are not many script support variables and not much variation
in them among scripts. If not overridden, this will assert support for
C<CONFIG>, C<HOST>, C<DISK>, C<MESSAGE-LINE>, C<EXECUTE-WHERE>, and (if
living in a recent-enough Amanda) C<TIMESTAMP>. If more script complexity
develops and warrants it, this should be expanded into a routine that calls
boolean I<supports_...> methods, as in C<Amanda::Application::Abstract>.

=cut

sub command_support {
    my ( $self ) = @_;

    $self->$say_supports(   	"CONFIG", "config");
    $self->$say_supports(   	  "HOST", "host");
    $self->$say_supports(   	  "DISK", "disk");
    $self->$say_supports( "MESSAGE-LINE", "message_line");
    $self->$say_supports("EXECUTE-WHERE", "execute_where");

    if ( defined $Amanda::Feature::fe_req_options_timestamp ) {
        $self->$say_supports("TIMESTAMP", "timestamp");
    }
}

package Amanda::Script::Message;
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
	last unless 'Amanda::Script::Abstract' eq $package;
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
    $app->print_to_server_and_die($self . '', $Amanda::Script_App::ERROR);
}

# An exception indicating the script has not been properly invoked.
package Amanda::Script::InvocationError;
use base 'Amanda::Script::Message';
sub local_message {
    my ( $self ) = @_;
    my $lm = 'usage error: ' . $self->{'item'};
    $lm .= ' (' . $self->{'value'} . ')' if defined $self->{'value'};
    $lm .= ': ' . $self->{'problem'};
    return $lm;
}

# An exception indicating some limit of the implementation has been hit (this
# includes where functionality that should be implemented simply isn't yet).
package Amanda::Script::ImplementationLimitError;
use base 'Amanda::Script::Message';
sub local_message {
    my ( $self ) = @_;
    my $lm = 'implementation limit: ' . $self->{'item'};
    $lm .= ' (' . $self->{'value'} . ')' if defined $self->{'value'};
    $lm .= ': ' . $self->{'problem'};
    return $lm;
}

# An exception indicating something is probably wrong in the implementation
# itself; an assertion/should-not-happen kind of thing.
package Amanda::Script::ImplementationError;
use base 'Amanda::Script::Message';
sub local_message {
    my ( $self ) = @_;
    my $lm = 'should not happen: ' . $self->{'item'};
    $lm .= ' (' . $self->{'value'} . ')' if defined $self->{'value'};
    $lm .= ': ' . $self->{'problem'};
    return $lm;
}

# An exception that reflects some condition in the environment where the
# application runs: missing files, I/O errors, the usual stuff that happens.
package Amanda::Script::EnvironmentError;
use base 'Amanda::Script::Message';
sub local_message {
    my ( $self ) = @_;
    my $lm = $self->{'item'};
    $lm .= ' (' . $self->{'value'} . ')' if defined $self->{'value'};
    $lm .= ': ' . $self->{'problem'} if defined $self->{'problem'};
    $lm .= ': ' . $self->{'errnostr'} if defined $self->{'errnostr'};
    return $lm;
}

# An exception reflecting a loss of numeric precision where that isn't ok.
package Amanda::Script::PrecisionLossError;
use base 'Amanda::Script::ImplementationLimitError';
sub new {
    my ( $class, %params ) = @_;
    $params{'problem'} = 'Loss of numeric precision'
	unless exists $params{'problem'};
    $class->SUPER::new(%params);
}

# An exception reflecting the status of a called process.
package Amanda::Script::CalledProcessError;
use base 'Amanda::Script::EnvironmentError';
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

# An exception bundling one or more other exceptions as an array {'exceptions'}.
package Amanda::Script::MultipleError;
use base 'Amanda::Script::Message';
sub local_message {
    my ( $self ) = @_;
    return 'Collected errors: ' . scalar(@{$self->{'exceptions'}});
}
sub on_uncaught {
    my ( $self, $app ) = @_;
    for my $exc ( @{$self->{'exceptions'}} ) {
	$app->print_to_server($exc . '', $Amanda::Script_App::ERROR);
    }
    $app->print_to_server_and_die($self . '', $Amanda::Script_App::ERROR);
}

1;
