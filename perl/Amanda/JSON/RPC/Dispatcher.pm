package Amanda::JSON::RPC::Dispatcher;
BEGIN {
  $Amanda::JSON::RPC::Dispatcher::VERSION = '0.0505';
}

=head1 NAME

Amanda::JSON::RPC::Dispatcher - A JSON-RPC 2.0 server.

=head1 VERSION

version 0.0505

=head1 SYNOPSIS

In F<app.psgi>:

 use Amanda::JSON::RPC::Dispatcher;

 my $rpc = Amanda::JSON::RPC::Dispatcher->new;

 sub add_em {
    my @params = @_;
    my $sum = 0;
    $sum += $_ for @params;
    return $sum;
 }
 $rpc->register( 'sum', \&add_em );

 $rpc->to_app;

Then run it:

 plackup app.psgi

Now you can then call this service via a GET like:

 http://example.com/?method=sum;params=[2,3,5];id=1

Or by posting JSON to it like this:

 {"jsonrpc":"2.0","method":"sum","params":[2,3,5],"id":"1"}

And you'd get back:

 {"jsonrpc":"2.0","result":10,"id":"1"}
 
=head1 DESCRIPTION

Using this app you can make any PSGI/L<Plack> aware server a JSON-RPC 2.0 server. This will allow you to expose your custom functionality as a web service in a relatiely tiny amount of code, as you can see above.

This module follows the draft specficiation for JSON-RPC 2.0. More information can be found at L<http://groups.google.com/group/json-rpc/web/json-rpc-2-0>.

=head2 Registration Options

The C<register> method cannot be used to register methods that start with m/^rpc\./.  Per the JSON-RPC 2.0 specification, these are reserved for
rpc-internal extensions.

The C<register> method takes a third argument which is a hash reference of named options that effects how the code should be handled.

=head3 with_plack_request

The first argument passed into the function will be a reference to the Plack::Request object, which is great for getting environment variables, and HTTP headers if you need those things in processing your RPC.

 $rpc->register( 'some_func', \&some_func, { with_plack_request => 1 });

 sub some_func {
     my ($plack_request, $other_arg) = @_;
     ...
 }

B<TIP:> Before using this option consider whether you might be better served by a L<Plack::Middleware> component. For example, if you want to do HTTP Basic Auth on your requests, use L<Plack::Middleware::Basic::Auth> instead. 

=head2 Advanced Error Handling

You can also throw error messages rather than just C<die>ing, which will throw an internal server error. To throw a specific type of error, C<die>, C<carp>, or C<confess>, an array reference starting with the error code, then the error message, and finally ending with error data (optional). When Amanda::JSON::RPC::Dispatcher detects this, it will throw that specific error message rather than a standard internal server error.

 use Amanda::JSON::RPC::Dispatcher;
 my $rpc = Amanda::JSON::RPC::Dispatcher->new;

 sub guess {
     my ($guess) = @_;
    if ($guess == 10) {
	    return 'Correct!';
    }
    elsif ($guess > 10) {
        die [986, 'Too high.'];
    }
    else {
        die [987, 'Too low.'];
    }
 }

 $rpc->register( 'guess', \&guess );

 $rpc->to_app;

B<NOTE:> If you don't care about setting error codes and just want to set an error message, you can simply C<die> in your RPC and your die message will be inserted into the C<error_data> method.

=head2 Logging

Amanda::JSON::RPC::Dispatcher allows for logging via L<Log::Any>. This way you can set up logs with L<Log::Dispatch>, L<Log::Log4perl>, or any other logging system that L<Log::Any> supports now or in the future. It's relatively easy to set up. In your F<app.psgi> simply add a block like this:

 use Log::Any::Adapter;
 use Log::Log4perl;
 Log::Log4perl::init('/path/to/log4perl.conf');
 Log::Any::Adapter->set('Log::Log4perl');

That's how easy it is to start logging. You'll of course still need to configure the F<log4perl.conf> file, which goes well beyond the scope of this document. And you'll also need to install L<Log::Any::Adapter::Log4perl> to use this example.

Amanda::JSON::RPC::Dispatcher logs the following:

=over

=item INFO

Requests and responses.

=item DEBUG

In the case when there is an unhandled exception, anything other than the error message will be put into a debug log entry.

=item TRACE

If an exception is thrown that has a C<trace> method, then it's contents will be put into a trace log entry.

=item ERROR

All errors that are gracefully handled by the system will be put into an error log entry.

=item FATAL

All errors that are not gracefully handled by the system will be put into a fatal log entry. Most of the time this means there's something wrong with the request document itself.

=back

=cut


use Moose;
use bytes;
extends qw(Plack::Component);
use Plack::Request;
use JSON -convert_blessed_universally;
use Amanda::JSON::RPC::Dispatcher::Procedure;
#use Log::Any;
use Log::Any qw($log);

#--------------------------------------------------------
has error_code => (
    is          => 'rw',
    default     => undef,
    predicate   => 'has_error_code',
    clearer     => 'clear_error_code',
);

#--------------------------------------------------------
has error_message => (
    is      => 'rw',
    default => undef,
    clearer => 'clear_error_message',
);

#--------------------------------------------------------
has error_data  => (
    is      => 'rw',
    default => undef,
    clearer => 'clear_error_data',
);

#--------------------------------------------------------
has rpcs => (
    is      => 'rw',
    default => sub { {} },
);

#--------------------------------------------------------
sub clear_error {
    my ($self) = @_;

    $self->clear_error_code;
    $self->clear_error_message;
    $self->clear_error_data;
}

#--------------------------------------------------------
sub register {
    my ($self, $name, $sub, $options) = @_;

    if(defined($name) && $name =~ m{^rpc\.}) {
        die "$name is an invalid name for a method. (Methods matching m/^rpc\\./ are reserved for rpc-internal procedures)";
    } elsif(!defined($name) || $name eq '' || ref($name)) {
        die "Registered method name must be a defined non-empty string and not start with 'rpc.'";
    }

    my $rpcs = $self->rpcs;
    $rpcs->{$name} = {
        function            => $sub,
        with_plack_request  => $options->{with_plack_request},
    };
    $self->rpcs($rpcs);
}

#--------------------------------------------------------
sub acquire_procedures {
    my ($self, $request) = @_;
    if ($request->method eq 'POST') {
        return $self->acquire_procedures_from_post($request);
    }
    elsif ($request->method eq 'GET') {
        return [ $self->acquire_procedure_from_get($request) ];
    }
    else {
        $self->error_code(-32600);
        $self->error_message('Invalid Request.');
        $self->error_data('Invalid method type: '.$request->method);
        return [];
    }
}

#--------------------------------------------------------
sub acquire_procedures_from_post {
    my ($self, $plack_request) = @_;
    my $body = $plack_request->content;
    my $request = eval{from_json($body, {utf8=>1})};
    if ($@) {
        $self->error_code(-32700);
        $self->error_message('Parse error.');
        $self->error_data($body);
        $log->fatal('Parse error.');
        $log->debug($body);
        return undef;
    }
    else {
        if (ref $request eq 'ARRAY') {
            my @procs;
            foreach my $proc (@{$request}) {
                push @procs, $self->create_proc($proc->{method}, $proc->{id}, $proc->{params}, $plack_request);
            }
            return \@procs;
        }
        elsif (ref $request eq 'HASH') {
            return [ $self->create_proc($request->{method}, $request->{id}, $request->{params}, $plack_request) ];
        }
        else {
            $self->error_code(-32600);
            $self->error_message('Invalid request.');
            $self->error_data($request);
            $log->fatal('Invalid request.');
            $log->debug($body);
            return undef;
        }
    }
}

#--------------------------------------------------------
sub acquire_procedure_from_get {
    my ($self, $plack_request) = @_;
    my $params = $plack_request->query_parameters;
    my $decoded_params = (exists $params->{params}) ? eval{from_json($params->{params},{utf8=>1})} : undef;
    return $self->create_proc($params->{method}, $params->{id}, ($@ || $decoded_params), $plack_request);
}

#--------------------------------------------------------
sub create_proc {
    my ($self, $method, $id, $params, $plack_request) = @_;
    my $proc = Amanda::JSON::RPC::Dispatcher::Procedure->new(
        method  => $method,
        id      => $id,
    );

    # process parameters
    if (defined $params) {
        unless (ref $params eq 'ARRAY' or ref $params eq 'HASH') {
            $proc->invalid_params($params);
            return $proc;
        }
    }
    my @vetted;
    if (ref $params eq 'HASH') {
        @vetted = (%{$params});
    }
    elsif (ref $params eq 'ARRAY') {
        @vetted = (@{$params});
    }
    if ($self->rpcs->{$proc->method}{with_plack_request}) {
        unshift @vetted, $plack_request;
    }
    $proc->params(\@vetted);
    return $proc;
}

#--------------------------------------------------------
sub translate_error_code_to_status {
    my ($self, $code) = @_;
    $code ||= '';
    my %trans = (
        ''          => 200,
        '-32600'    => 400,
        '-32601'    => 404,
    );
    my $status = $trans{$code};
    $status ||= 500;
    return $status;
}

#--------------------------------------------------------
sub handle_procedures {
    my ($self, $procs) = @_;
    my @responses;
    my $rpcs = $self->rpcs;
    foreach my $proc (@{$procs}) {
        my $is_notification = (defined $proc->id && $proc->id ne '') ? 0 : 1;
        unless ($proc->has_error_code) {
            my $rpc = $rpcs->{$proc->method};
            my $code_ref = $rpc->{function};
            if (defined $code_ref) {
                # deal with params and calling
                my $result = eval{ $code_ref->( @{ $proc->params } ) };

                # deal with result
                if ($@ && ref($@) eq 'ARRAY') {
                    $proc->error(@{$@});
                    $log->error($@->[1]);
                    $log->debug($@->[2]);
                }
                elsif ($@) {
                    my $error = $@;
                    if ($error->can('error') && $error->can('trace')) {
                         $log->fatal($error->error);
                         $log->trace($error->trace->as_string);
                         $error = $error->error;
                    }
                    elsif ($error->can('error')) {
                        $error = $error->error;
                        $log->fatal($error);
                    }
                    elsif (ref $error ne '' && ref $error ne 'HASH' && ref $error ne 'ARRAY') {
                        $log->fatal($error);
                        $error = ref $error;
                    }
                    $proc->internal_error($error);
                }
                else {
                    $proc->result($result);
                }
            }
            else {
                $proc->method_not_found($proc->method);
            }
        }

        # remove not needed elements per section 5 of the spec
        my $response = $proc->response;
        if (exists $response->{error}{code}) {
            delete $response->{result};
        }
        else {
            delete $response->{error};
        }

        # remove responses on notifications per section 4.1 of the spec
        unless ($is_notification) {
            push @responses, $response;
        }
    }

    # return the appropriate response, for batch or not
    if (scalar(@responses) > 1) {
        return \@responses;
    }
    else {
        return $responses[0];
    }
}

#--------------------------------------------------------
sub call {
    my ($self, $env) = @_;

    my $request = Plack::Request->new($env);
    $log->info("REQUEST: ".$request->content) if $log->is_info;
    $self->clear_error;
    my $procs = $self->acquire_procedures($request);

    my $rpc_response;
    if ($self->has_error_code) {
        $rpc_response = {
            jsonrpc => '2.0',
            error   => {
                code    => $self->error_code,
                message => $self->error_message,
                data    => $self->error_data,
            },
        };
    }
    else {
        $rpc_response = $self->handle_procedures($procs);
    }

    my $response = $request->new_response;
    if ($rpc_response and ref $rpc_response->{'result'} eq "CODE") {
	return $rpc_response->{'result'};
    } elsif ($rpc_response) {
        my $json = eval{JSON->new->convert_blessed->utf8->encode($rpc_response)};
        if ($@) {
            $log->error("JSON repsonse error: ".$@);
            $json = JSON->new->utf8->encode({
                jsonrpc => "2.0",
                error   => {
                    code    => -32099,
                    message => "Couldn't convert method response to JSON.",
                    data    => $@,
                    }
                 });
        }
        $response->status($self->translate_error_code_to_status( (ref $rpc_response eq 'HASH' && exists $rpc_response->{error}) ? $rpc_response->{error}{code} : '' ));
        $response->content_type('application/json-rpc');
        $response->content_length(bytes::length($json));
        $response->body($json);
        if ($response->status == 200) {
            $log->info("RESPONSE: ".$response->body) if $log->is_info;
        }
        else {
            $log->error("RESPONSE: ".$response->body);
        }
    }
    else { # is a notification only request
        $response->status(204);
        $log->info('RESPONSE: Notification Only');
    }
    return $response->finalize;
}

=head1 PREREQS

L<Moose> 
L<JSON> 
L<Plack>
L<Test::More>
L<Log::Any>

=head1 SUPPORT

=over

=item Repository

L<http://github.com/plainblack/JSON-RPC-Dispatcher>

=item Bug Reports

L<http://github.com/plainblack/JSON-RPC-Dispatcher/issues>

=back

=head1 SEE ALSO

You may also want to check out these other modules, especially if you're looking for something that works with JSON-RPC 1.x.

=over 

=item Dispatchers

Other modules that compete directly with this module, though perhaps on other protocol versions.

=over

=item L<Amanda::JSON::RPC>

An excellent and fully featured both client and server for JSON-RPC 1.1.

=item L<POE::Component::Server::JSONRPC>

A JSON-RPC 1.0 server for POE. I couldn't get it to work, and it doesn't look like it's maintained.

=item L<Catalyst::Plugin::Server::JSONRPC>

A JSON-RPC 1.1 dispatcher for Catalyst.

=item L<CGI-JSONRPC>

A CGI/Apache based JSON-RPC 1.1 dispatcher. Looks to be abandoned in alpha state. Also includes L<Apache2::JSONRPC>.

=item L<AnyEvent::JSONRPC::Lite>

An L<AnyEvent> JSON-RPC 1.x dispatcher. 

=item L<Sledge::Plugin::JSONRPC>

JSON-RPC 1.0 dispatcher for Sledge MVC framework.

=back

=item Clients

Modules that you'd use to access various dispatchers.

=over

=item L<Amanda::JSON::RPC::Common>

A JSON-RPC client for 1.0, 1.1, and 2.0. Haven't used it, but looks pretty feature complete.

=item L<RPC::JSON>

A simple and good looking Amanda::JSON::RPC 1.x client. I haven't tried it though.

=back

=back

=head1 AUTHOR

JT Smith <jt_at_plainblack_com>

=head1 LEGAL

Amanda::JSON::RPC::Dispatcher is Copyright 2009-2010 Plain Black Corporation (L<http://www.plainblack.com/>) and is licensed under the same terms as Perl itself.

=cut

1;
