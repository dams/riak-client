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
has connection_timeout => ( is => 'ro', isa => Num,  default  => sub {0.5} );

has driver => ( is => 'lazy' );

has _request_accumulator => (is => 'rw', init_arg => undef);

sub _reloop {
    die bless \do { my $e }, '__RELOOP__';
}

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
        Timeout  => $self->connection_timeout,
    );

    croak "Error ($!), can't connect to $host:$port"
      unless defined $socket;


    # TODO: handle timeout properly

    return $socket;
}

sub BUILD {
    $_[0]->driver;
}

# error
const my $ERROR_RESPONSE_CODE       => 0;
# ping
const my $PING_REQUEST_CODE         => 1;
const my $PING_RESPONSE_CODE        => 2;
# get, get_raw
const my $GET_REQUEST_CODE          => 9;
const my $GET_RESPONSE_CODE         => 10;
# put, put_raw
const my $PUT_REQUEST_CODE          => 11;
const my $PUT_RESPONSE_CODE         => 12;
# get_keys
const my $GET_KEYS_REQUEST_CODE     => 17;
const my $GET_KEYS_RESPONSE_CODE    => 18;
# del, del
const my $DEL_REQUEST_CODE          => 13;
const my $DEL_RESPONSE_CODE         => 14;
# quesry_index
const my $QUERY_INDEX_REQUEST_CODE  => 25;
const my $QUERY_INDEX_RESPONSE_CODE => 26;

sub ping {
    $_[0]->_parse_response(
        request_code  => $PING_REQUEST_CODE,
        expected_code => $PING_RESPONSE_CODE,
        operation_name => 'ping',
        body      => q(),
    );
}

sub is_alive {
    eval { $_[0]->ping };
}

sub get_keys {
    state $check = compile(Any, Str, Optional[CodeRef]);
    my ( $self, $bucket, $callback ) = $check->(@_);

    # reset accumulator
    $self->_request_accumulator([]);
    my $body = RpbListKeysReq->encode( { bucket => $bucket } );
    $self->_parse_response(
        request_code   => $GET_KEYS_REQUEST_CODE,
        expected_code  => $GET_KEYS_RESPONSE_CODE,
        key            => "*",
        bucket         => $bucket,
        operation_name => 'get_keys',
        body           => $body,
        callback       => $callback,
        handle_response => \&_handle_get_keys_response
    );
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
        push @{$self->_request_accumulator}, @keys;
        if ($obj->done) {
            my $keys = $self->_request_accumulator;
            $self->_request_accumulator([]);
            return $keys;
        }
    }
    # continuation
    _reloop();
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
        request_code  => $GET_REQUEST_CODE,
        expected_code => $GET_RESPONSE_CODE,
        key       => $key,
        bucket    => $bucket,
        operation_name => 'get',
        body      => $body,
        decode    => $decode,

        handle_response => \&_handle_get_response,
    );
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
        request_code   => $PUT_REQUEST_CODE,
        expected_code  => $PUT_RESPONSE_CODE,
        key            => $key,
        bucket         => $bucket,
        operation_name => 'put',
        body           => $body,
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
        request_code   => $DEL_REQUEST_CODE,
        expected_code  => $DEL_RESPONSE_CODE,
        key            => $key,
        bucket         => $bucket,
        operation_name => 'del',
        body           => $body,
    );
}

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

    $self->_parse_response(
        request_code   => $QUERY_INDEX_REQUEST_CODE,
        expected_code  => $QUERY_INDEX_RESPONSE_CODE,
        $query_type_is_eq ?
          (key => '2i query on ' . join('...', @$value_to_match) )
        : (key => $value_to_match ),
        bucket    => $bucket,
        operation_name => 'query_index',
        body      => $body,
        handle_response => \&_handle_query_index_response,
    );
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


sub _parse_response {
    my ( $self, %args ) = @_;
    
    my $operation_name = $args{operation_name};

    my $request_code  = $args{request_code};
    my $expected_code = $args{expected_code};

    my $request_body = $args{body};
    my $decode       = $args{decode};
    my $bucket       = $args{bucket};
    my $key          = $args{key};
    my $callback = $args{callback};

    my $handle_response = $args{handle_response};
    
    $self->driver->perform_request(
        $request_code, $request_body
      )
      or return $self->_die_generic_error(
        $ERRNO, $operation_name, $bucket,
        $key
      );

#    my $done = 0;
#$expected_code != $GET_KEYS_RESPONSE_CODE;

    while (1) {
        # get and check response
        my $response = $self->driver->read_response();

        my ($response_code, $response_body, $response_error) = @{$response}{qw(code body error)};

        # in case of internal error message
        defined $response_error
          and return $self->_die_generic_error(
              $response_error, $operation_name, $bucket,
              $key
          );
    
        # in case of error msg

        if ($response_code == $ERROR_RESPONSE_CODE) {
            my $decoded_message = RpbErrorResp->decode($response_body);
            my $errmsg  = $decoded_message->errmsg;
            my $errcode = $decoded_message->errcode;

            return $self->_die_generic_error( "Riak Error (code: $errcode) '$errmsg'",
                                                  $operation_name, $bucket, $key
                                                );
        }


        # check if we have what we want
        $response_code != $expected_code
          and return $self->_die_generic_error(
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

    my $extra = '';
    defined $bucket && defined $key
      and $extra = "(bucket: $bucket, key: $key)";

    croak "Error in '$operation_name' $extra: $error";
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

It started as a for of Riak::Light to fix some bugs, but actually ended up in a
complete rewrite with more features, but the same performance

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

Timeout for socket connection operation.

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
     my $key = $_; # also in $_[0]

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
the index value you're searching on, and optionally a callback.

If a callback has been provided, doesn't return anything, but execute the
callback on each matching keys. callback will receive the key name as first
argument. key name will also be in $_. If no callback is provided, returns and
ArrayRef of matching keys.

The index value you're searching on can be of two types. If it's a scalar, an
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

=head1 SEE ALSO

L<Net::Riak>

L<Data::Riak>

L<Data::Riak::Fast>

L<Action::Retry>

L<Riak::Light>
