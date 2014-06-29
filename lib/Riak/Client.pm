## no critic (RequireUseStrict, RequireUseWarnings)
package Riak::Client;
## use critic

use 5.010;
use Riak::Client::PBC;
use Type::Params qw(compile);
use Types::Standard -types;
use Errno qw(EINTR);
use Scalar::Util qw(blessed);
use JSON::XS;
use Carp;
use Module::Runtime qw(use_module);
require bytes;
use Moo;

use AnyEvent::Handle;

use IO::Socket::INET;
use IO::Socket::Timeout;

use Scalar::Util qw(weaken);

# ABSTRACT: Fast and lightweight Perl client for Riak

=head1 SYNOPSIS

  use Riak::Client;

  # normal mode
  my $client = Riak::Client->new(
    host => '127.0.0.1',
    port => 8087,
    r => 2,
    w => 2,
    dw => 1,
    connection_timeout => 5,
    read_timeout => 5,
    write_timeout => 5,
    no_auto_connect => 0,
  );

  # AnyEvent mode
  my $client = Riak::Client->new(
    host => '127.0.0.1',
    port => 8087
    anyevent_mode => 1,
  );

  $client->is_alive() or die "riak is not alive";

  # store hashref. will be serialized as JSON
  $client->put( 'bucket_name', 'key_name', { some => 'structure' } );

  # store text
  $client->put( 'bucket_name', 'key_name', 'sometext', 'text/plain' );

  # store raw data
  $client->put_raw( 'bucket_name', 'key_name', 'rawdata' );

  # fetch hashref
  my $hash = $client->get( 'bucket_name', 'key_name' );

  # fetch raw data
  my $text = $client->get_raw( 'bucket_name', 'key_name');

  # delete data
  $client->del( 'bucket_name', 'key_name');

  # AnyEvent mode
  my $cv = AE::cv
  $client->get_raw( 'bucket_name', 'key_name'
                    sub { do_something_with($_[0]);
                          $cv->send();
                    }
              );
  # ... later one
  $cv->recv();

  # list keys in stream
  $client->get_keys(foo => sub{
     my ($key, $done) = @_;

     # you should use another client inside this callback!
     $another_client->del(foo => $key);

  });
  
=head1 DESCRIPTION

Riak::Client is a fast and light Perl client for Riak using PBC interface, with
optional AnyEvent mode.

It supports operations like ping, get, exists, put, del, and secondary indexes
(so-called 2i) setting and querying.

It has two modes, a traditional procedural mode, and an event based mode, using
AnyEvent.

It started as a fork of C<Riak::Light> to fix some bugs, but actually ended up
in a complete rewrite with more features, but the same performance.

=attr anyevent_mode

Enables the AnyEvent mode, allowing true asynchronous mode.

=attr host

Str, Required. Riak IP or hostname.

=attr port

Int, Required. Port of the PBC interface.

=attr r

Int, Default 2. R value setting for this client.

=attr w

Int, Default 2. W value setting for this client.

=attr dw

Int, Default 1. DW value setting for this client.

=attr connection_timeout

Float, Default 5. Timeout for connection operation, in seconds. Set to 0 for no timeout.

=attr read_timeout

Float, Default 5. Timeout for read operation, in seconds. Set to 0 for no timeout.

=attr write_timeout

Float, Default 5. Timeout for write operation, in seconds. Set to 0 for no timeout.

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

Bool, Default 0. If set to true, then the module won't automatically connect upon instanciation.
Instead, you'll have to call C<connect()> yourself.

=cut

has no_auto_connect => ( is => 'ro', isa => Bool,  default  => sub {0} );

=attr anyevent_mode

Bool, Default 0. If set to true, then all methods can receive a callback,
as last argument. If present, the method will return immediately, and the
callback will be executed upon completion of the operation, receiving a condvar
as first and only argument. If set to false (the default), then the client
instance will be synchronous.

=cut

has anyevent_mode => ( is => 'ro', reader => 'ae', isa => Bool, default  => sub {0} );

has _cv_connected => ( is => 'ro', lazy => 1, default => sub { AE::cv });
has _requests_lock => ( is => 'rw', default => sub { undef });

has _handle => ( is => 'ro', lazy => 1, builder => 1 );
sub _build__handle {
    my ($self) = @_;
    my ($host, $port) = ($self->host, $self->port);
    my $handle;

    weaken $self;

    # TODO = timeouts
    $handle = AnyEvent::Handle->new (
      connect  => [$host, $port],
      no_delay => 1,
      on_error => sub {
         $handle->destroy; # explicitly destroy handle
         my $command = $self->{_req_command} // "<unknown>";
         my $bucket = $self->{_req_bucket} // "<unknown>";
         my $key = $self->{_req_key} // "<unknown>";
         croak("Error ($_[2]) on $host:$port, while performing: command '$command' on bucket '$bucket' and key '$key'");
      },
#      rtimeout => $self->read_timeout,
#      wtimeout => $self->write_timeout,
#      on_prepare => sub { $self->connection_timeout },
      on_connect => sub { $self->_cv_connected->send },
#      on_timeout => sub { print STDERR " ---- PLOP \n";},
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

    use Socket qw(IPPROTO_TCP TCP_NODELAY);
    $socket->setsockopt(IPPROTO_TCP, TCP_NODELAY, 1);

    return $socket;
}

sub BUILD {
    my ($self) = @_;
    $self->no_auto_connect
      and return;

    $self->connect();
}

=method connect

  my $client->connect();

  # or in AnyEvent mode
  $cv = AE::cv;
  $client->connect(sub { print "connected!\n"; $cv->send(); });
  $cv->recv();

Connects to the Riak server. This is automatically done when C<new()> is
called, unless the C<no_auto_connect> attribute is set to true. In AnyEvent
mode, returns a conditional variable,that will be triggered when connected.

=cut

sub connect {
    my ($self) = @_;

    $self->ae
      or $self->_socket(),
         return 1;

    $self->_handle();
    if (my $cb = ref $_[-1] eq 'CODE' ? $_[-1] : undef) {
        $self->_cv_connected->cb($cb);
        return;
    }

    $self->_cv_connected->recv;
    return 1;

}

has _getkeys_accumulator => (is => 'rw', init_arg => undef);
has _mapreduce_accumulator => (is => 'rw', init_arg => undef);

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
    # del
    DEL_REQUEST_CODE               => 13,
    DEL_RESPONSE_CODE              => 14,
    # get_buckets
    GET_BUCKETS_REQUEST_CODE       => 15,
    GET_BUCKETS_RESPONSE_CODE      => 16,
    # get_keys
    GET_KEYS_REQUEST_CODE          => 17,
    GET_KEYS_RESPONSE_CODE         => 18,
    # get_bucket_props
    GET_BUCKET_PROPS_REQUEST_CODE  => 19,
    GET_BUCKET_PROPS_RESPONSE_CODE => 20,
    # set_bucket_props
    SET_BUCKET_PROPS_REQUEST_CODE  => 21,
    SET_BUCKET_PROPS_RESPONSE_CODE => 22,
    # map_reducd
    MAP_REDUCE_REQUEST_CODE        => 23,
    MAP_REDUCE_RESPONSE_CODE       => 24,
    # query_index
    QUERY_INDEX_REQUEST_CODE       => 25,
    QUERY_INDEX_RESPONSE_CODE      => 26,
};

=method ping

  use Try::Tiny;
  try { $client->ping() } catch { "oops... something is wrong: $_" };

Perform a ping operation. Will die in case of error. See C<is_alive()>

=cut

sub ping {
    state $check = compile(Any, Optional[CodeRef]);
    my ( $self, $cb ) = $check->(@_);
    $_[0]->_parse_response( {
        request_code   => PING_REQUEST_CODE,
        expected_code  => PING_RESPONSE_CODE,
        operation_name => 'ping',
        body_ref       => \'',
        cb             => $cb,
    } );
}

=method is_alive

  $client->is_alive() or warn "oops... something is wrong: $@";

Perform a ping operation. Will return false in case of error (which will be stored in $@).

=cut

sub is_alive {
    state $check = compile(Any, Optional[CodeRef]);
    my ( $self, $cb ) = $check->(@_);
    my $res = eval { $self->ping; 1 };
    $cb and return $cb->($res);
    return $res;
}

=method get

  # blocking mode
  my $value = $client->get(bucket => 'key');

  # blocking mode, with a callback
  $client->get(bucket => 'key', sub { my ($value) = @_; do_stuff($value) });

  # AnyEvent mode, asynchronous
  $cv = AE::cv;
  $client->get(bucket => 'key', sub { my ($value) = @_; do_stuff($value); $cv->send() });
  # ...
  $cv->recv();

Perform a fetch operation. Expects bucket and key names. If the content_type of
the fetched value is 'application/json', automatically decodes the JSON into a
Perl structure. If you need the raw data you can use L<get_raw>.

=cut

sub get {
    state $check = compile(Any, Str, Str, Optional[CodeRef]);
    my ( $self, $bucket, $key, $cb ) = $check->(@_);
    $self->_fetch( $bucket, $key, 1, 0, $cb );
}

=method get_raw

  my $scalar_value = $client->get_raw(bucket => 'key');

Perform a fetch operation. Expects bucket and key names. Returns the raw data.
If you need decode the json, you should use C<get()> instead.

=cut

sub get_raw {
    state $check = compile(Any, Str, Str, Optional[CodeRef]);
    my ( $self, $bucket, $key, $cb ) = $check->(@_);
    $self->_fetch( $bucket, $key, 0, 0, $cb );
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
    my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
    state $check = compile(Any, Str, Str, Any, Optional[Str], Optional[HashRef[Str]]);
    my ( $self, $bucket, $key, $value, $content_type, $indexes ) = $check->(@_);

    ($content_type //= 'application/json')
      eq 'application/json'
        and $value = encode_json($value);

    $self->_store( $bucket, $key, $value, $content_type, $indexes, $cb);
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
    my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
    state $check = compile(Any, Str, Str, Any, Optional[Str], Optional[HashRef[Str]]);
    my ( $self, $bucket, $key, $value, $content_type, $indexes ) = $check->(@_);
    $content_type ||= 'plain/text';
    $self->_store( $bucket, $key, $value, $content_type, $indexes, $cb);
}

=method del

  $client->del(bucket => key);

Perform a delete operation. Expects bucket and key names.

=cut

sub del {
    state $check = compile(Any, Str, Str, Optional[CodeRef]);
    my ( $self, $bucket, $key, $cb ) = $check->(@_);

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
        cb             => $cb,
    } );
}

=method get_keys

  # in default mode
  $client->get_keys(foo => sub{
     my ($key, $done) = @_;
     # you should use another client inside this callback!
     $another_client->del(foo => $key);
  });

  # in anyevent mode
  my $cv = AE::cv;
  $client->get_keys(foo => sub{
     my ($key, $done) = @_;
     # ... do stuff with $key
     $done and $cv->send;
  });
  $cv->recv();

B<WARNING>, this method should not be called on a production Riak cluster, as
it can have a big performance impact. See Riak's documentation.

B<WARNING>, because Riak doesn't handles pipelining, you cannot use the same
C<Riak::Client> instance inside the callback, it would raise an exception.

Perform a list keys operation. Receive a callback and will call it for each
key. The callback will receive two arguments: the key, and a boolean indicating
if it's the last key

The callback is optional, in which case an ArrayRef of B<all> the keys are
returned. But don't do that, and always provide a callback, to avoid your RAM
usage to skyrocket...

=cut

sub get_keys {
    state $check = compile(Any, Str, Optional[CodeRef]);
    my ( $self, $bucket, $cb ) = $check->(@_);

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
        cb             => $cb,
        handle_response => \&_handle_get_keys_response,
        lock_requests => 1,
    } );
}

sub _handle_get_keys_response {
    my ( $self, $encoded_message, $args ) = @_;

    # TODO: support for 1.4 (which provides 'stream', 'return_terms', and 'stream')
    my $obj = RpbListKeysResp->decode( $encoded_message );
    my @keys = @{$obj->keys // []};

    # case 1 : no user callback
    my $cb = $args->{cb};
    if (! $cb ) {
        # accumulate results
        push @{$self->_getkeys_accumulator}, @keys;

        # if more to come, return by saying so
        $obj->done
          or return (undef, 1);

        # all results are there, return the whole
        my $keys = $self->_getkeys_accumulator;
        $self->_getkeys_accumulator([]);
        return \$keys;
    }

    # case 2 : we have a user callback
    my $last_key;
    my $obj_done = $obj->done
      and $last_key = pop @keys;

    # no second arg = more to come
    $cb->($_) foreach @keys;

    # if more to come, return by saying so
    $obj->done
      or return (undef, 1);

    # process last keys if any
    defined $last_key and $cb->($last_key, 1);

    # means: nothing left to do, all results processed through callback
    return;
}

=method exists

  $client->exists(bucket => 'key') or warn "key not found";
  
Perform a fetch operation but with head => 0, and the if there is something
stored in the bucket/key.

=cut

sub exists {
    state $check = compile(Any, Str, Str, Optional[CodeRef]);
    my ( $self, $bucket, $key, $cb ) = $check->(@_);
    $self->_fetch( $bucket, $key, 0, 1, $cb );
}

sub _fetch {
    my ( $self, $bucket, $key, $decode, $test_exist, $cb ) = @_;

    my $body = RpbGetReq->encode(
        {   r      => $self->r,
            key    => $key,
            bucket => $bucket,
            head   => $test_exist
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
        test_exist => $test_exist,
        cb => $cb,
    } );
}

sub _handle_get_response {
    my ( $self, $encoded_message, $args ) = @_;

    defined $encoded_message
      or _die_generic_error( "Undefined Message", 'get', $args );

    my $decoded_message = RpbGetResp->decode($encoded_message);
    my $content = $decoded_message->content;

    # empty content
    ref $content eq 'ARRAY'
      or return \undef;

    # if we just need to test existence
    $args->{test_exist}
      and return \1;

    # TODO: handle metadata
    my $value        = $content->[0]->value;
    my $content_type = $content->[0]->content_type;

    # if we need to decode
    $args->{decode} && $content_type eq 'application/json'
      and return \decode_json($value);

    # simply return the value
    return \$value;
}

sub _store {
    my ( $self, $bucket, $key, $encoded_value, $content_type, $indexes, $cb ) = @_;

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
        cb             => $cb,
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
    my ( $self, $bucket, $index, $value_to_match, $cb ) = $check->(@_);

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
        cb => $cb,
        lock_requests => 1,
    } );
}

sub _handle_query_index_response {
    my ( $self, $encoded_message, $args ) = @_;
    
    my $obj = RpbIndexResp->decode( $encoded_message );
    my @keys = @{$obj->keys // []};

    # case 1 : no user callback
    my $cb = $args->{cb}
      or return \\@keys;

    # case 2 : we have a user callback
    $cb->($_) foreach @keys;

    # means: nothing left to do, all results processed through callback
    return;

}

=method get_buckets

B<WARNING>, this method should not be called on a production Riak cluster, as
it can have a big performance impact. See Riak's documentation.

=cut

sub get_buckets {
    state $check = compile(Any, Optional[CodeRef]);
    my ( $self, $cb ) = $check->(@_);

    $self->_parse_response( {
        request_code    => GET_BUCKETS_REQUEST_CODE,
        expected_code   => GET_BUCKETS_RESPONSE_CODE,
        operation_name  => 'get_buckets',
        handle_response => \&_handle_get_buckets_response,
        cb              => $cb,
    } );
}

sub _handle_get_buckets_response {
    my ( $self, $encoded_message, $args ) = @_;
    my $obj = RpbListBucketsResp->decode( $encoded_message );
    return \($obj->buckets // []);
}

=method get_bucket_props


=cut

sub get_bucket_props {
    state $check = compile(Any, Str, Optional[CodeRef]);
    my ( $self, $bucket, $cb ) = $check->(@_);

    my $body = RpbGetBucketReq->encode( { bucket => $bucket } );
    $self->_parse_response( {
        request_code    => GET_BUCKET_PROPS_REQUEST_CODE,
        expected_code   => GET_BUCKET_PROPS_RESPONSE_CODE,
        bucket          => $bucket,
        body_ref        => \$body,
        handle_response => \&_handle_get_bucket_props_response,
        cb              => $cb,
    } );
}

sub _handle_get_bucket_props_response {
    my ( $self, $encoded_message, $args ) = @_;

    my $obj = RpbListBucketsResp->decode( $encoded_message );
    my $props = RpbBucketProps->decode($obj->buckets->[0]);
    return \{ %$props }; # unblessing variable
}

=method set_bucket_props


=cut

sub set_bucket_props {
    state $check = compile( Any, Str, 
                            Dict[ n_val => Optional[Int],
                                  allow_mult => Optional[Bool] ],
                            Optional[CodeRef] );
    my ( $self, $bucket, $props, $cb ) = $check->(@_);
    $props->{n_val} && $props->{n_val} < 0 and croak 'n_val should be possitive integer';

    my $body = RpbSetBucketReq->encode({ bucket => $bucket, props => $props });
    $self->_parse_response( {
        request_code   => SET_BUCKET_PROPS_REQUEST_CODE,
        expected_code  => SET_BUCKET_PROPS_RESPONSE_CODE,
        bucket         => $bucket,
        body_ref       => \$body,
    } );
}

=method map_reduce

=cut

sub map_reduce {
  state $check = compile(Any, Any, Optional[CodeRef]);
  my ( $self, $request, $cb) = $check->(@_); 

  my @args;
  
  push @args, ref($request) ? encode_json($request): $request;
  push @args, 'application/json';
  push @args, $cb if $cb;
  
  map_reduce_raw($self, @args);

}

=method map_reduce_raw

=cut

sub map_reduce_raw {
  state $check = compile(Any, Str, Str, Optional[CodeRef]);
  my ( $self, $request, $content_type, $cb) = $check->(@_);
  
  my $body = RpbMapRedReq->encode(
    {
      request => $request,
      content_type => $content_type,
    }
  );

  # reset accumulator
  $self->_mapreduce_accumulator([]);

  $self->_parse_response( {
      request_code   => MAP_REDUCE_REQUEST_CODE,
      expected_code  => MAP_REDUCE_RESPONSE_CODE,
      operation => 'map_reduce',
      body_ref  => \$body,
      cb        => $cb,
      decode    => ($content_type eq 'application/json'),
      handle_response => \&_handle_map_reduce_response,
      lock_requests => 1,
  } );
}

sub _handle_map_reduce_response {
    my ( $self, $encoded_message, $args ) = @_;
    my $obj = RpbMapRedResp->decode( $encoded_message );
          
    # case 1 : no user callback
    my $cb = $args->{cb};
    if (! $cb ) {

        # all results were there, reset the accumulator and return the whole, 
        if ($obj->done) {
            my $results = $self->_mapreduce_accumulator();
            $self->_mapreduce_accumulator([]);
            return \$results;
        }

        # accumulate results
        push @{$self->_mapreduce_accumulator},
          { phase => $obj->phase, response => ($args->{decode}) ? decode_json($obj->response // '[]') : $obj->response };

        # more stuff to come, say so
        return (undef, 1);

    }

    # case 2 : we have a user callback

    # means: nothing left to do, all results processed through callback
    $obj->done
      and return;

    $cb->($obj->response, $obj->phase, $obj->done);

    # more stuff to come, say so
    return (undef, 1);

}

sub _parse_response {
    my ( $self, $args ) = @_;

    $self->ae
      and goto &_parse_response_ae;

    my $socket = $self->_socket;
    _send_bytes($socket, $args->{request_code}, $args->{body_ref} // \'');

    while (1) {
        my $response;
        # get and check response
        my $raw_response_ref = _read_response($socket)
          or _die_generic_error( $! || "Socket Closed", $args);

        my ( $response_code, $response_body ) = unpack( 'c a*', $$raw_response_ref );

        # in case of error msg
        if ($response_code == ERROR_RESPONSE_CODE) {
            my $decoded_message = RpbErrorResp->decode($response_body);
            my $errmsg  = $decoded_message->errmsg;
            my $errcode = $decoded_message->errcode;

            _die_generic_error( "Riak Error (code: $errcode) '$errmsg'", $args);
        }


        # check if we have what we want
        $response_code != $args->{expected_code}
          and _die_generic_error(
              "Unexpected Response Code in (got: $response_code, expected: $args->{expected_code})",
              $args );
    
        # default value if we don't need to handle the response.
        my ($ret, $more_to_come) = ( \1, undef);

        # handle the response.
        if (my $handle_response = $args->{handle_response}) {
            ($ret, $more_to_come) = $handle_response->( $self, $response_body, $args);
        }

        # it's a multiple response request, loop again
        $more_to_come
          and next;

        # there is a result, process or return it
        if ($ret) {
            $args->{cb} and return $args->{cb}->($$ret);
            return $$ret;
        }

        # ret was undef, means we have processed everything in the callback
        return;

    }
}

sub _parse_response_ae {
    my ( $self, $args ) = @_;


    # OK so Riak doesn't support pipelining. That means that you can't send a
    # request before the previous one has returned. Especially true for
    # multiple response requests, like get_keys. So we need a way to detect
    # that a running request is occuring, and we can't push_read before the
    # previous request is done.

    # if there is a lock on the requests
    if ($self->_requests_lock) {
        # wait to acquire lock
        $self->_requests_lock->recv();
        # delete the lock
        $self->_requests_lock(undef);
    }

    # if this request can have multiple responses, set the lock.
    $args->{lock_requests}
      and $self->_requests_lock(AE::cv);

    my $body_ref     = $args->{body_ref} // \'';
    
    $self->_handle->push_write(pack('N',
          bytes::length($$body_ref) + 1)
        . pack('c', $args->{request_code}) . $$body_ref
    );

    # maybe we'll use a cv to force synchronous call
    my $cv;
    # if we don't have a user callback, we need to be synchronous, create the cv.
    $args->{cb}
      or $cv = AE::cv;

    my $reader_handler;
    $reader_handler = sub {

        # length arrived, decode
        my $len = unpack "N", $_[1];
        # now read the payload
        $_[0]->unshift_read( chunk => $len, sub {
            my ( $response_code, $response_body ) = unpack( 'c a*', $_[1] );

            # in case of error msg
            if ($response_code == ERROR_RESPONSE_CODE) {
                my $decoded_message = RpbErrorResp->decode($response_body);
                my $errmsg  = $decoded_message->errmsg;
                my $errcode = $decoded_message->errcode;

                _die_generic_error( "Riak Error (code: $errcode) '$errmsg'", $args );
            }

            # check if we have what we want
            $response_code != $args->{expected_code}
              and _die_generic_error(
                      "Unexpected Response Code in (got: $response_code, expected: $args->{expected_code})",
                      $args );

            # default value if we don't need to handle the response.
            my ($ret, $more_to_come) = ( \1, undef);
            # remember, $handle_response may or may not use $args->{cb}
            if (my $handle_response = $args->{handle_response}) {
                ($ret, $more_to_come) = $handle_response->( $self, $response_body, $args );
            }
            # if we expect more to come, re-prepend the handler
            $more_to_come and $_[0]->unshift_read( chunk => 4, $reader_handler ),
                              return;

            # ok, single or multiple response are over, remove the lock.
            # This is done before last callback execution, so that user can
            # re-enqueue a request right away.
            my $lock = $self->_requests_lock;
            $lock and $lock->send();

            # if no user callback provided, use the $cv and return.
            $cv and $cv->send($ret),
                    return;

            # If $ret is undef, means everything has been processed and
            # callback called in $handle_response, nothing left to do.
            # Otherwise, we have a result, call the callback on it
            $ret and $args->{cb}->($$ret);
            
          });
    };

    # OK, now try to read from the socket with the handler
    $self->_handle->push_read( chunk => 4, $reader_handler);

    # we were given a user callback, don't be synchronous, immediately return.
    $cv or return;

    # no user callback, let's be synchronous
    my $res = $cv->recv();
    $res and
      return $$res;

    # $res was undef, that's an error here
    _die_generic_error( "internal error: response handler returns <undef>, but not in callback mode",
                               $args );
}

sub _die_generic_error {
    my ( $error, $args ) = @_;

    my ($operation_name, $bucket, $key) =
      map { $_ // "<unknown $_>" }
      @{$args}{qw( operation_name bucket key)};

    my $extra = '';
    defined $bucket && defined $key
      and $extra = "(bucket: $bucket, key: $key) ";

    croak "Error in '$operation_name' $extra: $error";
}

sub _read_response {
    my ($socket) = @_;
    _read_bytes($socket, unpack( 'N', ${ _read_bytes($socket, 4) // return } ));
}

sub _read_bytes {
    my ( $socket, $length ) = @_;

    my $buffer;
    my $offset = 0;
    my $read = 0;

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
    my ( $socket, $request_code, $body_ref ) = @_;

    my $bytes = pack('N', my $length = (bytes::length($$body_ref) + 1)) . pack('c', $request_code) . $$body_ref;

    $length += 4;
    my $offset = 0;
    my $sent = 0;

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

=for Pod::Coverage BUILD

=head1 CONTRIBUTORS

Ivan Kruglov <ivan.kruglov@yahoo.com>

=head1 SEE ALSO

L<Net::Riak>

L<Data::Riak>

L<Data::Riak::Fast>

L<Action::Retry>

L<Riak::Light>

L<AnyEvent>
