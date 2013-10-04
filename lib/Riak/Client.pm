## no critic (RequireUseStrict, RequireUseWarnings)
package Riak::Client;
## use critic

use 5.010;
use Riak::Client::PBC;
use Riak::Client::Driver;
use Type::Params qw(compile);
use Types::Standard -types;
use English qw(-no_match_vars );
use Scalar::Util qw(blessed);
use IO::Socket;
use Const::Fast;
use JSON;
use Carp;
use Module::Runtime qw(use_module);
use Moo;

# ABSTRACT: Fast and lightweight Perl client for Riak

has port    => ( is => 'ro', isa => Int,  required => 1 );
has host    => ( is => 'ro', isa => Str,  required => 1 );
has r       => ( is => 'ro', isa => Int,  default  => sub {2} );
has w       => ( is => 'ro', isa => Int,  default  => sub {2} );
has dw      => ( is => 'ro', isa => Int,  default  => sub {2} );
has timeout => ( is => 'ro', isa => Num,  default  => sub {0.5} );
has in_timeout  => ( is => 'lazy', trigger => 1 );
has out_timeout => ( is => 'lazy', trigger => 1 );

sub _trigger_in_timeout {
  carp "this feature will be disabled in the next version, you should use just timeout instead";
}

sub _trigger_out_timeout {
  carp "this feature will be disabled in the next version, you should use just timeout instead";
}

sub _build_in_timeout {
    $_[0]->timeout;
}

sub _build_out_timeout {
    $_[0]->timeout;
}

has timeout_provider => (
    is => 'ro',
    isa => Maybe [Str],
    default => sub {'Riak::Client::Timeout::Select'}
);

has driver => ( is => 'lazy' );

sub _build_driver {
    Riak::Client::Driver->new( socket => $_[0]->_build_socket() );
}

sub _build_socket {
    my ($self) = @_;

    my $host = $self->host;
    my $port = $self->port;

    my $socket = IO::Socket::INET->new(
        PeerHost => $host,
        PeerPort => $port,
        Timeout  => $self->timeout,
    );

    croak "Error ($!), can't connect to $host:$port"
      unless defined $socket;

    return $socket unless defined $self->timeout_provider;

    use Module::Load qw(load);
    load $self->timeout_provider;

    # TODO: add a easy way to inject this proxy
    $self->timeout_provider->new(
        socket      => $socket,
        in_timeout  => $self->in_timeout,
        out_timeout => $self->out_timeout,
    );
}

sub BUILD {
    $_[0]->driver;
}

const my $PING     => 'ping';
const my $GET      => 'get';
const my $PUT      => 'put';
const my $DEL      => 'del';
const my $GET_KEYS => 'get_keys';
const my $QUERY_INDEX => 'query_index';

const my $ERROR_RESPONSE_CODE    => 0;
const my $GET_RESPONSE_CODE      => 10;
const my $GET_KEYS_RESPONSE_CODE => 18;
const my $QUERY_INDEX_RESPONSE_CODE => 26;

const my $CODES => {
        $PING     => { request_code => 1,  response_code => 2 },
        $GET      => { request_code => 9,  response_code => 10 },
        $PUT      => { request_code => 11, response_code => 12 },
        $DEL      => { request_code => 13, response_code => 14 },
        $GET_KEYS => { request_code => 17, response_code => 18 },
        $QUERY_INDEX => { request_code => 25, response_code => 26 },
    };


sub ping {
    $_[0]->_parse_response(
        operation => $PING,
        body      => q(),
    );
}

sub is_alive {
    eval { $_[0]->ping };
}

sub get_keys {
    state $check = compile(Any, Str, Optional[CodeRef]);
    my ( $self, $bucket, $callback ) = $check->(@_);

    my $body = RpbListKeysReq->encode( { bucket => $bucket } );
    $self->_parse_response(
        key       => "*",
        bucket    => $bucket,
        operation => $GET_KEYS,
        body      => $body,
        callback => $callback,
    );
}

sub get_raw {
    state $check = compile(Any, Str, Str);
    my ( $self, $bucket, $key ) = $check->(@_);
    $self->_fetch( $bucket, $key, 0 );
}

sub get {
    state $check = compile(Any, Str, Str);
    my ( $self, $bucket, $key ) = $check->(@_);
    $self->_fetch( $bucket, $key, 1 );
}

sub exists {
    state $check = compile(Any, Str, Str);
    my ( $self, $bucket, $key ) = $check->(@_);
    defined $self->_fetch( $bucket, $key, 0, 1 );
}

sub _fetch {
    my ( $self, $bucket, $key, $decode, $head ) = @_;

    my $body = RpbGetReq->encode(
        {   r      => $self->r,
            key    => $key,
            bucket => $bucket,
            head   => $head
        }
    );

    $self->_parse_response(
        key       => $key,
        bucket    => $bucket,
        operation => $GET,
        body      => $body,
        decode    => $decode,
    );
}

sub put_raw {
    state $check = compile(Any, Str, Str, Any, Optional[Str], Optional[HashRef[Str]]);
    my ( $self, $bucket, $key, $value, $content_type, $indexes ) = $check->(@_);
    $content_type ||= 'plain/text';
    $self->_store( $bucket, $key, $value, $content_type, $indexes);
}

sub put {
    state $check = compile(Any, Str, Str, Any, Optional[Str], Optional[HashRef[Str]]);
    my ( $self, $bucket, $key, $value, $content_type, $indexes ) = $check->(@_);

    ($content_type ||= 'application/json')
      eq 'application/json'
        and $value = encode_json($value);

    $self->_store( $bucket, $key, $value, $content_type, $indexes);
}

sub _store {
    my ( $self, $bucket, $key, $encoded_value, $content_type, $indexes ) = @_;

    my $body = RpbPutReq->encode(
        {   key     => $key,
            bucket  => $bucket,
            content => {
                value        => $encoded_value,
                content_type => $content_type,
                ( $indexes ?
                  (indexes => [
                              map {
                                  { key => $_ , value => $indexes->{$_} }
                              } keys %$indexes
                             ])
                  : () ),
            },
        }
    );

    $self->_parse_response(
        key       => $key,
        bucket    => $bucket,
        operation => $PUT,
        body      => $body,
    );
}

sub del {
    state $check = compile(Any, Str, Str);
    my ( $self, $bucket, $key ) = $check->(@_);

    my $body = RpbDelReq->encode(
        {   key    => $key,
            bucket => $bucket,
            rw     => $self->dw
        }
    );

    $self->_parse_response(
        key       => $key,
        bucket    => $bucket,
        operation => $DEL,
        body      => $body,
    );
}

sub query_index {
     state $check = compile(Any, Str, Str, Str|ArrayRef);
     my ( $self, $bucket, $index, $value_to_match ) = $check->(@_);

     my $query_type = 0; # eq
     ref $value_to_match
       and $query_type = 1; # range
     my $body = RpbIndexReq->encode(
         {   index    => $index,
             bucket   => $bucket,
             qtype    => $query_type,
             $query_type ?
             ( range_min => $value_to_match->[0],
               range_max => $value_to_match->[1] )
             : (key => $value_to_match ),
         }
     );

     $self->_parse_response(
         $query_type ?
           (key => '2i query on ' . $value_to_match->[0] . '...' . $value_to_match->[1])
         : (key => $value_to_match ),
         bucket    => $bucket,
         operation => $QUERY_INDEX,
         body      => $body,
     );
 }

sub _parse_response {
    my ( $self, %args ) = @_;
    
    my $operation = $args{operation};

    my $request_code  = $CODES->{$operation}->{request_code};
    my $expected_code = $CODES->{$operation}->{response_code};

    my $request_body = $args{body};
    my $decode       = $args{decode};
    my $bucket       = $args{bucket};
    my $key          = $args{key};
    my $callback = $args{callback};
    
    $self->driver->perform_request(
        code => $request_code,
        body => $request_body
      )
      or return $self->_process_generic_error(
        $ERRNO, $operation, $bucket,
        $key
      );

#    my $done = 0;
#$expected_code != $GET_KEYS_RESPONSE_CODE;

    my $response;
    my @results;
    while (1) {
        # get and check response
        $response = $self->driver->read_response()
          // { code => -1, body => undef, error => $ERRNO };

        my ($response_code, $response_body, $response_error) = @{$response}{qw(code body error)};

        # in case of internal error message
        defined $response_error
          and return $self->_process_generic_error(
              $response_error, $operation, $bucket,
              $key
          );
    
        # in case of error msg
        $response_code == $ERROR_RESPONSE_CODE
          and return $self->_process_riak_error(
              $response_body, $operation, $bucket,
              $key
          );
    
        # in case of default message
        $response_code != $expected_code
          and return $self->_process_generic_error(
              "Unexpected Response Code in (got: $response_code, expected: $expected_code)",
              $operation, $bucket, $key
          );
    
        # we have a 'get' response
        $response_code == $GET_RESPONSE_CODE
          and return $self->_process_get_response( $response_body, $bucket, $key, $decode );

        # we have a 'get_keys' response
        # TODO: support for 1.4 (which provides 'stream', 'return_terms', and 'stream')
        if ($response_code == $GET_KEYS_RESPONSE_CODE) {
            my $obj = RpbListKeysResp->decode( $response_body );
            my @keys = @{$obj->keys // []};
            if ($callback) {
                $callback->($_) foreach @keys;
                $obj->done
                  and return;
            } else {
                push @results, @keys;
                $obj->done
                  and return \@results;
            }
            next;
        }

        # in case of a 'query_index' response
        if ($response_code == $QUERY_INDEX_RESPONSE_CODE) {
            my $obj = RpbIndexResp->decode( $response_body );
            my @keys = @{$obj->keys // []};
            if ($callback) {
                $callback->($_) foreach @keys;
                return;
            } else {
                return \@keys;
            }
            next;
        }

        # in case of no return value, signify success
        return 1;
    }

}

sub _process_get_response {
    my ( $self, $encoded_message, $bucket, $key, $decode ) = @_;

    $self->_process_generic_error( "Undefined Message", 'get', $bucket, $key )
      unless ( defined $encoded_message );

    my $should_decode   = $decode;
    my $decoded_message = RpbGetResp->decode($encoded_message);

    my $content = $decoded_message->content;
    if ( ref($content) eq 'ARRAY' ) {
        my $value        = $content->[0]->value;
        my $content_type = $content->[0]->content_type;

        return ( $content_type eq 'application/json' and $should_decode )
          ? decode_json($value)
          : $value;
    }

    undef;
}

sub _process_riak_error {
    my ( $self, $encoded_message, $operation, $bucket, $key ) = @_;

    my $decoded_message = RpbErrorResp->decode($encoded_message);

    my $errmsg  = $decoded_message->errmsg;
    my $errcode = $decoded_message->errcode;

    $self->_process_generic_error(
        "Riak Error (code: $errcode) '$errmsg'",
        $operation, $bucket, $key
    );
}

sub _process_generic_error {
    my ( $self, $error, $operation, $bucket, $key ) = @_;

    my $extra =
      ( $operation ne 'ping' )
      ? "(bucket: $bucket, key: $key)"
      : q();

    my $error_message = "Error in '$operation' $extra: $error";

    croak $error_message;

    $@ = $error_message;    ## no critic (RequireLocalizedPunctuationVars)

    undef;
}

1;

__END__

=head1 SYNOPSIS

  use Riak::Client;

  # create a new instance - using pbc only
  my $client = Riak::Client->new(
    host => '127.0.0.1',
    port => 8087
  );

  $client->is_alive() or die "ops, riak is not alive";

  # store hashref into bucket 'foo', key 'bar'
  # will serializer as 'application/json'
  $client->put( foo => bar => { baz => 1024 });

  # store text into bucket 'foo', key 'bar' 
  $client->put( foo => baz => "sometext", 'text/plain');
  $client->put_raw( foo => baz => "sometext");  # does not encode !

  # fetch hashref from bucket 'foo', key 'bar'
  my $hash = $client->get( foo => 'bar');
  my $text = $client->get_raw( foo => 'baz');   # does not decode !

  # delete hashref from bucket 'foo', key 'bar'
  $client->del(foo => 'bar');

  # check if exists (like get but using less bytes in the response)
  $client->exists(foo => 'baz') or warn "ops, foo => bar does not exist";

  # list keys in stream (callback only)
  $client->get_keys(foo => sub{
     my $key = $_[0];

     # you should use another client inside this callback!
     $another_client->del(foo => $key);
  });
  
=head1 DESCRIPTION

Riak::Client is a very light (and fast) Perl client for Riak using PBC
interface. Support operations like ping, get, exists, put, del, and secondary
indexes (so-called 2i) setting and querying.

It is flexible to change the timeout backend for I/O operations. There is no
auto-reconnect option. It can be very easily wrapped up by modules like
L<Action::Retry> to manage flexible retry/reconnect strategies.

=head2 ATTRIBUTES

=head3 host

Riak ip or hostname. There is no default.

=head3 port

Port of the PBC interface. There is no default.

=head3 r

R value setting for this client. Default 2.

=head3 w

W value setting for this client. Default 2.

=head3 dw

DW value setting for this client. Default 2.

=head3 timeout

Timeout for connection, write and read operations. Default is 0.5 seconds.

=head3 in_timeout

Timeout for read operations. Default is timeout value.

=head3 out_timeout

Timeout for write operations. Default is timeout value.

=head3 timeout_provider

Can change the backend for timeout. The default value is IO::Socket::INET and
there is only support to connection timeout.

B<IMPORTANT>: in case of any timeout error, the socket between this client and the
Riak server will be closed. To support I/O timeout you can choose 5 options (or
you can set undef to avoid IO Timeout):

=over  

=item * Riak::Client::Timeout::Alarm

uses alarm and Time::HiRes to control the I/O timeout. Does not work on Win32.
(Not Safe)

=item * Riak::Client::Timeout::Time::Out

uses Time::Out and Time::HiRes to control the I/O timeout. Does not work on
Win32. (Not Safe)

=item *  Riak::Client::Timeout::Select

uses IO::Select to control the I/O timeout

=item *  Riak::Client::Timeout::SelectOnWrite

uses IO::Select to control only Output Operations. Can block in Write
Operations. Be Careful.

=item *  Riak::Client::Timeout::SetSockOpt

uses setsockopt to set SO_RCVTIMEO and SO_SNDTIMEO socket properties. Does not
Work on NetBSD 6.0.

=back

=head3 driver

This is a Riak::Client::Driver instance, to be able to connect and perform
requests to Riak over PBC interface.

=head2 METHODS


=head3 is_alive

  $client->is_alive() or warn "ops... something is wrong: $@";

Perform a ping operation. Will return false in case of error (will store in $@).

=head3 is_alive

  try { $client->ping() } catch { "oops... something is wrong: $_" };

Perform a ping operation. Will die in case of error.

=head3 get

  my $value_or_reference = $client->get(bucket => 'key');

Perform a fetch operation. Expects bucket and key names. Decode the json into a
Perl structure. if the content_type is 'application/json'. If you need the raw
data you can use L<get_raw>.

=head3 get_raw

  my $scalar_value = $client->get_raw(bucket => 'key');

Perform a fetch operation. Expects bucket and key names. Return the raw data.
If you need decode the json, you should use L<get> instead.

=head3 exists

  $client->exists(bucket => 'key') or warn "key not found";
  
Perform a fetch operation but with head => 0, and the if there is something
stored in the bucket/key.

=head3 put

  $client->put('bucket', 'key', { some_values => [1,2,3] });
  $client->put('bucket', 'key', { some_values => [1,2,3] }, 'application/json);
  $client->put('bucket', 'key', 'text', 'plain/text');

  # you can set secondary indexes (2i)
  $client->put( 'bucket', 'key', 'text', 'plain/text',
                { field1_bin => 'abc', field2_int => 42 }
              );
  $client->put( 'bucket', 'key', { some_values => [1,2,3] }, undef,
                { field1_bin => 'abc', field2_int => 42 }
              );

Perform a store operation. Expects bucket and key names, the value, the content
type (optional, default is 'application/json'), and the indexes to set for this
value (optional, default is none).

Will encode the structure in json string if necessary. If you need only store
the raw data you can use L<put_raw> instead.

B<IMPORTANT>: all the index field names should end by either C<_int> or
C<_bin>, depending if the index type is integer or binary.

To query secondary indexes, see L<query_index>.

=head3 put_raw

  $client->put_raw('bucket', 'key', encode_json({ some_values => [1,2,3] }), 'application/json');
  $client->put_raw('bucket', 'key', 'text');
  $client->put_raw('bucket', 'key', 'text', undef, {field_bin => 'foo'});

Perform a store operation. Expects bucket and key names, the value, the content
type (optional, default is 'plain/text'), and the indexes to set for this value
(optional, default is none).

Will encode the raw data. If you need encode the structure you can use L<put>
instead.

B<IMPORTANT>: all the index field names should end by either C<_int> or
C<_bin>, depending if the index type is integer or binary.

To query secondary indexes, see L<query_index>.

=head3 del

  $client->del(bucket => key);

Perform a delete operation. Expects bucket and key names.
  
=head3 get_keys

  $client->get_keys(foo => sub{
     my $key = $_[0];

     # you should use another client inside this callback!
     $another_client->del(foo => $key);
  });

Perform a list keys operation. Receive a callback and will call it for each
key. You can't use this callback to perform other operations!

The callback is optional, in which case an ArrayRef of all the keys are
returned. But you should always provide a callback, to avoid your RAM usage to
skyrocket...

=head3 query_index

Perform a secondary index query. Expects a bucket name, the index field name,
and the index value you're searching on. Returns and ArrayRef of matching keys.

The index value you're searching on can be of two types. If it's a scalar, an
B<exact match> query will be performed. if the value is an ArrayRef, then a
B<range> query will be performed, the first element in the array will be the
range_min, the second element the range_max. other elements will be ignored.

Based on the example in C<put>, here is how to query it:

  # exact match
  my $matching_keys = $client->query_index( 'bucket',  'field2_int', 42 ),

  # range match
  my $matching_keys = $client->query_index( 'bucket',  'field2_int', [ 40, 50] ),

=head1 SEE ALSO

L<Net::Riak>

L<Data::Riak>

L<Data::Riak::Fast>

L<Action::Retry>
