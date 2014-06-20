## no critic (RequireUseStrict, RequireUseWarnings)
package Riak::Client;
## use critic

use 5.010;
use Riak::Client::PBC;
use Type::Params qw(compile);
use Types::Standard -types;
use English qw(-no_match_vars );
use Errno qw(EINTR);
use Scalar::Util qw(blessed);
use JSON;
use Carp;
use Module::Runtime qw(use_module);
require bytes;
use Moo;

use AnyEvent::Handle;

use IO::Socket::INET;
use IO::Socket::Timeout;


# ABSTRACT: Fast and lightweight Perl client for Riak

=head1 SYNOPSIS

  use Riak::Client;

  # create a new instance - using pbc only
  my $client = Riak::Client->new(
    host => '127.0.0.1',
    port => 8087
  );

  $client->is_alive() or die "oops, riak is not alive";

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
  $client->exists(foo => 'baz') or warn "oops, foo/bar does not exist";

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

It started as a for of Riak::Light to fix some bugs, but actually ended up in a
complete rewrite with more features, but the same performance

=attr host

Riak IP or hostname. Str, Required.

=attr port

Port of the PBC interface. Int, Required.

=attr r

R value setting for this client. Int, Default 2.

=attr w

W value setting for this client. Int, Default 2.

=attr dw

DW value setting for this client. Int, Default 1.

=attr connection_timeout

Timeout for connection operation, in seconds. Set to 0 for no timeout. Float, Defaults to 5

=attr read_timeout

Timeout for read operation, in seconds. Set to 0 for no timeout. Float, Defaults to 5

=attr write_timeout

Timeout for write operation, in seconds. Set to 0 for no timeout. Float, Defaults to 5

=cut

has host    => ( is => 'ro', isa => Str,  required => 1 );
has port    => ( is => 'ro', isa => Int,  required => 1 );
has r       => ( is => 'ro', isa => Int,  default  => sub {2} );
has w       => ( is => 'ro', isa => Int,  default  => sub {2} );
has dw      => ( is => 'ro', isa => Int,  default  => sub {1} );
has connection_timeout => ( is => 'ro',                 isa => Num,  default  => sub {5} );
has read_timeout       => ( is => 'ro', predicate => 1, isa => Num,  default  => sub {5} );
has write_timeout      => ( is => 'ro', predicate => 1, isa => Num,  default  => sub {5} );

=attr no_auto_connect

If set to true, then the module won't automatically connect upon instanciation.
Instead, you'll have to call C<connect()> yoruself. Boolean, Defaults to 0

=cut

has no_auto_connect => ( is => 'ro', isa => Bool,  default  => sub {0} );

=attr anyevent_mode

Bool, Defaults to 0. If set to true, then the client instance will return
AnyEvent condvars instead, and thus become completely asynchronous. If set to
false (the default), then the client instance will be synchronous.

=cut

has anyevent_mode => ( is => 'ro', reader => 'ae', isa => Bool, default  => sub {0} );

has _cv_connected => ( is => 'ro', lazy => 1, default => sub { AE::cv });

has _handle => ( is => 'ro', lazy => 1, builder => 1 );
sub _build__handle {
    my ($self) = @_;
    my ($host, $port) = ($self->host, $self->port);
    my $handle;

    # TODO = timeouts
    $handle = AnyEvent::Handle->new (
      connect  => [$host, $port],
      on_error => sub {
         $handle->destroy; # explicitly destroy handle
         # $self->_cv_connected->croak("Error ($!) on $host:$port");
         croak("Error ($!) on $host:$port");
      },
      rtimeout => $self->read_timeout,
      wtimeout => $self->write_timeout,
      on_prepare => sub { $self->connection_timeout },
      on_connect => sub { $self->_cv_connected->send },
    );

}

has _socket => ( is => 'ro', lazy => 1, builder => 1 );
sub _build__socket {
    my ($self) = @_;

    my $host = $self->host;
    my $port = $self->port;

    my $socket = IO::Socket::INET->new(
        PeerHost => $host,
        PeerPort => $port,
        Timeout  => $self->connection_timeout,
    );

    croak "Error ($!), can't connect to $host:$port"
      unless defined $socket;

    $self->has_read_timeout || $self->has_write_timeout
      or return $socket;

    # enable read and write timeouts on the socket
    IO::Socket::Timeout->enable_timeouts_on($socket);
    # setup the timeouts
    $self->has_read_timeout
      and $socket->read_timeout($self->read_timeout);
    $self->has_write_timeout
      and $socket->write_timeout($self->write_timeout);

    return $socket;
}

sub BUILD {
    my ($self) = @_;
    $self->no_auto_connect
      or $self->connect();
}

=method connect

  my $client->connect();

  # or in AnyEvent mode
  my $cv = $client->connect();

Connects to the Riak server. This is automatically done when C<new()> is
called, unless the C<no_auto_connect> attribute is set to true. In AnyEvent
mode, returns a conditional variable,that will be triggered when connected.

=cut

sub connect {
    my ($self) = @_;

    if ($self->ae) {
        $self->_handle();
        return $self->_cv_connected;
    }

    $self->_socket();
    return 1;
}

has _getkeys_accumulator => (is => 'rw', init_arg => undef);

sub _reloop {
    die bless \do { my $e }, '__RELOOP__';
}

use constant {
    # error
    ERROR_RESPONSE_CODE            => 0,
    # ping
    PING_REQUEST_CODE              => 1,
    PING_RESPONSE_CODE             => 2,
    # get, get_raw
    GET_REQUEST_CODE               => 9,
    GET_RESPONSE_CODE              => 10,
    # put, put_raw
    PUT_REQUEST_CODE               => 11,
    PUT_RESPONSE_CODE              => 12,
    # get_buckets
    GET_BUCKETS_REQUEST_CODE       => 15,
    GET_BUCKETS_RESPONSE_CODE      => 16,
    # get_keys
    GET_KEYS_REQUEST_CODE          => 17,
    GET_KEYS_RESPONSE_CODE         => 18,
    # del
    DEL_REQUEST_CODE               => 13,
    DEL_RESPONSE_CODE              => 14,
    # get_bucket_props
    GET_BUCKET_PROPS_REQUEST_CODE  => 19,
    GET_BUCKET_PROPS_RESPONSE_CODE => 20,
    # set_bucket_props
    SET_BUCKET_PROPS_REQUEST_CODE  => 21,
    SET_BUCKET_PROPS_RESPONSE_CODE => 22,
    # quesry_index
    QUERY_INDEX_REQUEST_CODE       => 25,
    QUERY_INDEX_RESPONSE_CODE      => 26,
};

=method ping

  use Try::Tiny;
  try { $client->ping() } catch { "oops... something is wrong: $_" };

Perform a ping operation. Will die in case of error. See C<is_alive()>

=cut

sub ping {
    $_[0]->_parse_response( {
        request_code  => PING_REQUEST_CODE,
        expected_code => PING_RESPONSE_CODE,
        operation_name => 'ping',
        body_ref      => \q(),
    } );
}

=method is_alive

  $client->is_alive() or warn "oops... something is wrong: $@";

Perform a ping operation. Will return false in case of error (which will be stored in $@).

=cut

sub is_alive {
    eval { $_[0]->ping };
}

=method get

  # standard blocking mode
  my $value = $client->get(bucket => 'key');

  # asynchronous, with a callback
  $client->get(bucket => 'key', sub { my ($value) = @_; do_stuff($value) });

  # asynchronous, in AnyEvent mode
  my $cv = $client->get(bucket => 'key');

Perform a fetch operation. Expects bucket and key names. If the content_type is
'application/json', decodes the JSON into a Perl structure. . If you need the
raw data you can use L<get_raw>.

=cut

sub get {
    state $check = compile(Any, Str, Str);
    my ( $self, $bucket, $key ) = $check->(@_);
    $self->_fetch( $bucket, $key, 1 );
}

=method get_raw

  my $scalar_value = $client->get_raw(bucket => 'key');

Perform a fetch operation. Expects bucket and key names. Returns the raw data.
If you need decode the json, you should use C<get()> instead.

=cut

sub get_raw {
    state $check = compile(Any, Str, Str);
    my ( $self, $bucket, $key ) = $check->(@_);
    $self->_fetch( $bucket, $key, 0 );
}

=method put

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

=cut

sub put {
    state $check = compile(Any, Str, Str, Any, Optional[Str], Optional[HashRef[Str]]);
    my ( $self, $bucket, $key, $value, $content_type, $indexes ) = $check->(@_);

    ($content_type ||= 'application/json')
      eq 'application/json'
        and $value = encode_json($value);

    $self->_store( $bucket, $key, $value, $content_type, $indexes);
}


=method put_raw

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

=cut

sub put_raw {
    state $check = compile(Any, Str, Str, Any, Optional[Str], Optional[HashRef[Str]]);
    my ( $self, $bucket, $key, $value, $content_type, $indexes ) = $check->(@_);
    $content_type ||= 'plain/text';
    $self->_store( $bucket, $key, $value, $content_type, $indexes);
}

=method del

  $client->del(bucket => key);

Perform a delete operation. Expects bucket and key names.

=cut

sub del {
    state $check = compile(Any, Str, Str);
    my ( $self, $bucket, $key ) = $check->(@_);

    my $body = RpbDelReq->encode(
        {   key    => $key,
            bucket => $bucket,
            rw     => $self->dw
        }
    );

    $self->_parse_response( {
        request_code   => DEL_REQUEST_CODE,
        expected_code  => DEL_RESPONSE_CODE,
        operation_name => 'del',
        key            => $key,
        bucket         => $bucket,
        body_ref       => \$body,
    } );
}

=method get_keys

  $client->get_keys(foo => sub{
     my $key = $_; # also in $_[0]

     # you should use another client inside this callback!
     $another_client->del(foo => $key);
  });

Perform a list keys operation. Receive a callback and will call it for each
key.

The callback is optional, in which case an ArrayRef of B<all> the keys are
returned. But don't do that, and always provide a callback, to avoid your RAM
usage to skyrocket...

=cut

sub get_keys {
    state $check = compile(Any, Str, Optional[CodeRef]);
    my ( $self, $bucket, $callback ) = $check->(@_);

    # reset accumulator
    $self->_getkeys_accumulator([]);
    my $body = RpbListKeysReq->encode( { bucket => $bucket } );
    $self->_parse_response( {
        request_code   => GET_KEYS_REQUEST_CODE,
        expected_code  => GET_KEYS_RESPONSE_CODE,
        operation_name => 'get_keys',
        key            => "*",
        bucket         => $bucket,
        body_ref       => \$body,
        callback       => $callback,
        handle_response => \&_handle_get_keys_response
    } );
}

sub _handle_get_keys_response {
    my ( $self, $encoded_message, $bucket, $key, $callback ) = @_;

    # TODO: support for 1.4 (which provides 'stream', 'return_terms', and 'stream')
    my $obj = RpbListKeysResp->decode( $encoded_message );
    my @keys = @{$obj->keys // []};
    if ($callback) {
        $callback->($_) foreach @keys;
        $obj->done
          and return;
    } else {
        push @{$self->_getkeys_accumulator}, @keys;
        if ($obj->done) {
            my $keys = $self->_getkeys_accumulator;
            $self->_getkeys_accumulator([]);
            return $keys;
        }
    }
    # continuation
    _reloop();
}

=method exists

  $client->exists(bucket => 'key') or warn "key not found";
  
Perform a fetch operation but with head => 0, and the if there is something
stored in the bucket/key.

=cut

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

    $self->_parse_response( {
        request_code  => GET_REQUEST_CODE,
        expected_code => GET_RESPONSE_CODE,
        operation_name => 'get',
        key       => $key,
        bucket    => $bucket,
        body_ref  => \$body,
        decode    => $decode,
        handle_response => \&_handle_get_response,
    } );
}

sub _handle_get_response {
    my ( $self, $encoded_message, $bucket, $key, undef, $decode ) = @_;

    defined $encoded_message
      or $self->_die_generic_error( "Undefined Message", 'get', $bucket, $key );

    my $should_decode   = $decode;
    my $decoded_message = RpbGetResp->decode($encoded_message);

    my $content = $decoded_message->content;

    # empty content
    ref($content) eq 'ARRAY'
      or return undef;

    # TODO: handle metadata
    my $value        = $content->[0]->value;
    my $content_type = $content->[0]->content_type;

    # if we need to decode
    $content_type eq 'application/json' && $should_decode
      and return decode_json($value);

    # simply return the value
    return $value;
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

    $self->_parse_response( {
        request_code   => PUT_REQUEST_CODE,
        expected_code  => PUT_RESPONSE_CODE,
        operation_name => 'put',
        key            => $key,
        bucket         => $bucket,
        body_ref       => \$body,
    } );
}

=method query_index

Perform a secondary index (2i) query. Expects a bucket name, the index field
name, the index value you're searching on, and optionally a callback.

If a callback has been provided, doesn't return anything, but execute the
callback on each matching keys. callback will receive the key name as first
argument. key name will also be in C<$_>. If no callback is provided, returns
and ArrayRef of matching keys.

The index value you're searching on can be of two types. If it's a Scalar, an
B<exact match> query will be performed. if the value is an ArrayRef, then a
B<range> query will be performed, the first element in the array will be the
range_min, the second element the range_max. other elements will be ignored.

Based on the example in C<put>, here is how to query it:

  # exact match
  my $matching_keys = $client->query_index( 'bucket',  'field2_int', 42 ),

  # range match
  my $matching_keys = $client->query_index( 'bucket',  'field2_int', [ 40, 50] ),

  # range match with callback
  $client->query_index( 'bucket',  'field2_int', [ 40, 50], sub { print "key : $_" } ),

=cut

sub query_index {
    state $check = compile(Any, Str, Str, Str|ArrayRef, Optional[CodeRef]);
    my ( $self, $bucket, $index, $value_to_match, $callback ) = $check->(@_);

    my $query_type_is_eq = 0; # eq
    ref $value_to_match
      and $query_type_is_eq = 1; # range
    my $body = RpbIndexReq->encode(
        {   index    => $index,
            bucket   => $bucket,
            qtype    => $query_type_is_eq,
            $query_type_is_eq ?
            ( range_min => $value_to_match->[0],
              range_max => $value_to_match->[1] )
            : (key => $value_to_match ),
            callback => $callback
        }
    );

    $self->_parse_response( {
        request_code   => QUERY_INDEX_REQUEST_CODE,
        expected_code  => QUERY_INDEX_RESPONSE_CODE,
        operation_name => 'query_index',
        $query_type_is_eq ?
          (key => '2i query on ' . join('...', @$value_to_match) )
        : (key => $value_to_match ),
        bucket    => $bucket,
        body_ref  => \$body,
        handle_response => \&_handle_query_index_response,
    } );
}

sub _handle_query_index_response {
    my ( $self, $encoded_message, $bucket, $key, $callback ) = @_;
    
    defined $encoded_message
      or $self->_die_generic_error( "Undefined Message", 'query_index', $bucket, $key );

    my $obj = RpbIndexResp->decode( $encoded_message );

    my @keys = @{$obj->keys // []};
    if ($callback) {
        $callback->($_) foreach @keys;
        return;
    } else {
        return \@keys;
    }
}

sub get_buckets {
    state $check = compile(Any, Optional[CodeRef]);
    my ( $self, $callback ) = $check->(@_);

    $self->_parse_response( {
        request_code    => GET_BUCKETS_REQUEST_CODE,
        expected_code   => GET_BUCKETS_RESPONSE_CODE,
        operation_name => 'get_buckets',
        callback        => $callback,
        handle_response => \&_handle_get_buckets_response,
    } );
}

sub _handle_get_buckets_response {
    my ( $self, $encoded_message, $bucket, $key, $callback ) = @_;

    defined $encoded_message
      or $self->_die_generic_error( "Undefined Message", 'get_buckets' );

    my $obj = RpbListBucketsResp->decode( $encoded_message );
    my @buckets = @{$obj->buckets // []};
    if ($callback) {
        $callback->($_) foreach @buckets;
        return;
    } else {
        return \@buckets;
    }
}

sub get_bucket_props {
    state $check = compile(Any, Str);
    my ( $self, $bucket ) = $check->(@_);

    my $body = RpbGetBucketReq->encode( { bucket => $bucket } );
    $self->_parse_response( {
        request_code    => GET_BUCKET_PROPS_REQUEST_CODE,
        expected_code   => GET_BUCKET_PROPS_RESPONSE_CODE,
        bucket          => $bucket,
        body_ref        => \$body,
        handle_response => \&_handle_get_bucket_props_response,
    } );
}

sub _handle_get_bucket_props_response {
    my ( $self, $encoded_message, $bucket ) = @_;

    defined $encoded_message
      or $self->_die_generic_error( "Undefined Message", 'get_bucket_props', $bucket );

    my $obj = RpbListBucketsResp->decode( $encoded_message );
    my $props = RpbBucketProps->decode($obj->buckets->[0]);
    return { %$props }; # unblessing variable
}

sub set_bucket_props {
    state $check = compile(Any, Str, Dict[ n_val => Optional[Int], allow_mult => Optional[Bool] ]);
    my ( $self, $bucket, $props ) = $check->(@_);
    $props->{n_val} && $props->{n_val} < 0 and croak 'n_val should be possitive integer';

    my $body = RpbSetBucketReq->encode({ bucket => $bucket, props => $props });
    $self->_parse_response( {
        request_code   => SET_BUCKET_PROPS_REQUEST_CODE,
        expected_code  => SET_BUCKET_PROPS_RESPONSE_CODE,
        bucket         => $bucket,
        body_ref       => \$body,
    } );
}

sub _parse_response {
    my ( $self, $args ) = @_;

    my $operation_name = $args->{operation_name};

    my $request_code  = $args->{request_code};
    my $expected_code = $args->{expected_code};

    my $body_ref     = $args->{body_ref};
    my $decode       = $args->{decode};
    my $bucket       = $args->{bucket};
    my $key          = $args->{key};
    my $callback     = $args->{callback};

    my $handle_response = $args->{handle_response};
    
    $body_ref //= \'';

    if ($self->ae) {
        $self->_handle->push_write(pack('N', bytes::length($$body_ref) + 1) . pack('c', $request_code) . $$body_ref);
    } else {
        $self->_send_bytes($request_code, $body_ref);
    }

    my $cv = AE::cv;

#    my $done = 0;
#$expected_code != GET_KEYS_RESPONSE_CODE;

    while (1) {
        my $response;
        # get and check response
        if (my $raw_response_ref = $self->read_response()) {
            my ( $code, $body ) = unpack( 'c a*', $$raw_response_ref );
            $response = { code => $code, body => $body, error => undef };
        } else {
            $response = { code => -1,
                          body => undef,
                          error => $ERRNO || "Socket Closed" };
        }

        my ($response_code, $response_body, $response_error) = @{$response}{qw(code body error)};

        # in case of internal error message
        defined $response_error
          and $self->_die_generic_error(
              $response_error, $operation_name, $bucket,
              $key
          );
    
        # in case of error msg

        if ($response_code == ERROR_RESPONSE_CODE) {
            my $decoded_message = RpbErrorResp->decode($response_body);
            my $errmsg  = $decoded_message->errmsg;
            my $errcode = $decoded_message->errcode;

            $self->_die_generic_error( "Riak Error (code: $errcode) '$errmsg'",
                                       $operation_name, $bucket, $key
                                     );
        }


        # check if we have what we want
        $response_code != $expected_code
          and $self->_die_generic_error(
              "Unexpected Response Code in (got: $response_code, expected: $expected_code)",
              $operation_name, $bucket, $key
          );
    
        # if we don't need to handle the response, return success
        $handle_response
          or return 1;

        # handle the response.
        my $ret;
        eval { $ret = $handle_response->(
                        $self,
                        $response_body,
                        $bucket,
                        $key,
                        $callback,
                        $decode );
                1; }
          and return $ret;

        # there were an exception. Is it a continuation or a real exception ?

        ref $@ eq '__RELOOP__'
          or die $@;
    }
}


sub _die_generic_error {
    my ( $self, $error, $operation_name, $bucket, $key ) = @_;

    { no strict 'refs';
      ${$_} //= "<unknown $_>"
        for qw(operation_name error bucket key);
    }

    my $extra = '';
    defined $bucket && defined $key
      and $extra = "(bucket: $bucket, key: $key)";

    croak "Error in '$operation_name' $extra: $error";
}

sub read_response_ae {
    my ($self)   = @_;
    my $cv = AE::cv;
    $self->_handle->push_read(
        chunk => 4,
        sub {
            # length arrived, decode
            my $len = unpack "N", $_[1];
            # now read the payload
            $_[0]->unshift_read(
                chunk => $len,
                sub {
                    my $data = $_[1];
                    $cv->send(\$data);
                    # handle xml
                });
        });
    my $data_ref = $cv->recv();
    return $data_ref;
}

sub read_response {
    my ($self) = @_;
    $self->_read_bytes(unpack( 'N', ${ $self->_read_bytes(4) // return } ));
}

sub _read_bytes {
    my ( $self, $length ) = @_;

    my $buffer;
    my $offset = 0;
    my $read = 0;
    my $socket = $self->_socket;

    while ($length > 0) {
        $read = $socket->sysread( $buffer, $length, $offset );
        if (! defined $read) {
            $! == EINTR
              and next;
            return;
        }

        $read > 0
          or return;

        $offset += $read;
        $length -= $read;
    }

    return \$buffer;
}


sub _send_bytes {
    my ( $self, $request_code, $body_ref ) = @_;

    my $bytes = pack('N', my $length = (bytes::length($$body_ref) + 1)) . pack('c', $request_code) . $$body_ref;

    $length += 4;
    my $offset = 0;
    my $sent = 0;
    my $socket = $self->_socket;

    while ($length > 0) {
        $sent = $socket->syswrite( $bytes, $length, $offset );
        if (! defined $sent) {
            $! == EINTR
              and next;
            return;
        }

        $sent > 0
          or return;

        $offset += $sent;
        $length -= $sent;
    }

    return $offset;
}



1;

=head1 SEE ALSO

L<Net::Riak>

L<Data::Riak>

L<Data::Riak::Fast>

L<Action::Retry>

L<Riak::Light>
