BEGIN {
    unless ( $ENV{RIAK_PBC_HOST} ) {
        require Test::More;
        Test::More::plan(
            skip_all => 'variable RIAK_PBC_HOST is not defined' );
    }
}

use Test::More tests => 5;
use Test::Exception;
use Riak::Client;
use JSON;

subtest "simple get/set/delete test" => sub {
    plan tests => 12;

    my ( $host, $port ) = split ':', $ENV{RIAK_PBC_HOST};

    my $client = Riak::Client->new(
        host             => $host, port => $port,
        timeout_provider => undef
    );

    my $scalar = '3.14159';
    my $hash = { baz => 1024 };

    ok( $client->ping(),     "can ping" );
    ok( $client->is_alive(), "is_alive" );
    ok( $client->put( foo => "bar", $hash ),
        "should store the hashref in Riak"
    );
    is_deeply(
        $client->get( foo => 'bar' ), $hash,
        "should fetch the stored hashref from Riak"
    );

    ok( $client->put_raw( foo => "bar2", $scalar ),
        "should store the raw scalar in Riak"
    );
    is( $client->get_raw( foo => 'bar2' ), $scalar,
        "should fetch the raw scalar from Riak"
    );

    ok( $client->exists( foo => 'bar' ), "should exists" );
    ok( $client->del( foo => 'bar' ), "should delete the hashref" );
    ok( !$client->get( foo => 'bar' ), "should fetch UNDEF from Riak" );
    ok( !$client->exists( foo => 'bar' ), "should not exists" );

    ok( $client->put( foo => "baz", 'TEXT', 'plain/text' ),
        "should store the text in Riak"
    );
    is( $client->get( foo => "baz" ), 'TEXT',
        "should fetch the text from Riak"
    );

    #ok(!$@, "should has no error - foo => bar is undefined");
};

subtest "get keys" => sub {
    plan tests => 4;

    my $bucket = "foo_" . int( rand(1024) ) . "_" . int( rand(1024) );

    my ( $host, $port ) = split ':', $ENV{RIAK_PBC_HOST};

    my $client = Riak::Client->new(
        host             => $host, port => $port,
        timeout_provider => undef
    );

    my @keys;
    $client->get_keys( $bucket => sub { push @keys, $_[0] } );

    foreach my $key (@keys) {
        $client->del( $bucket => $key );
    }
    my $hash = { a => 1 };

    $client->put( $bucket => "bar", $hash );
    $client->put( $bucket => "baz", $hash );
    $client->put( $bucket => "bam", $hash );

    @keys = ();
    $client->get_keys( $bucket => sub { push @keys, $_[0] } );

    @keys = sort @keys;
    is( scalar @keys, 3 );
    is( $keys[0],     'bam' );
    is( $keys[1],     'bar' );
    is( $keys[2],     'baz' );
};

subtest "sequence of 1024 get/set" => sub {
    plan tests => 1024;

    my ( $host, $port ) = split ':', $ENV{RIAK_PBC_HOST};

    my $client = Riak::Client->new(
        host             => $host, port => $port,
        timeout_provider => undef
    );

    my $hash = {
        foo       => bar  => baz     => 123,
        something => very => complex => [ 1, 2, 3, 4, 5 ]
    };

    my ( $bucket, $key );
    for ( 1 .. 1024 ) {
        ( $bucket, $key ) =
          ( "bucket" . int( rand(1024) ), "key" . int( rand(1024) ) );

        $hash->{random} = int( rand(1024) );

        $client->put( $bucket => $key => $hash );

        my $got_complex_structure = $client->get( $bucket => $key );
        is_deeply(
            $got_complex_structure, $hash,
            "get($bucket=>$key)should got the same structure"
        );
    }
};

subtest "get buckets" => sub {
    plan tests => 4;

    my ( $host, $port ) = split ':', $ENV{RIAK_PBC_HOST};

    my $client = Riak::Client->new(
        host             => $host, port => $port,
        timeout_provider => undef
    );

    my @new_buckets = (
        "foo_" . int( rand(1024) ) . "_" . int( rand(1024) ),
        "foo_" . int( rand(1024) ) . "_" . int( rand(1024) ),
        "foo_" . int( rand(1024) ) . "_" . int( rand(1024) ),
    );

    my @exp_buckets = ( @{ $client->get_buckets() // [] }, @new_buckets);

    my $key = "key" . int( rand(1024) );
    my $hash = { a => 1 };
    $client->put( $_ => $key => $hash ) foreach (@new_buckets);

    my @buckets = @{ $client->get_buckets() // [] };

    is( scalar @buckets, scalar @exp_buckets );

    foreach my $bucket (@new_buckets) {
        is(grep( $bucket eq $_, @buckets), 1, "bucket $bucket is not found");
    }
};

subtest "get/set buckets props" => sub {
    plan tests => 4;

    my ( $host, $port ) = split ':', $ENV{RIAK_PBC_HOST};

    my $client = Riak::Client->new(
        host             => $host, port => $port,
        timeout_provider => undef
    );

    my @buckets = (
        "foo_" . int( rand(1024) ) . "_" . int( rand(1024) ),
        "foo_" . int( rand(1024) ) . "_" . int( rand(1024) ),
        "foo_" . int( rand(1024) ) . "_" . int( rand(1024) ),
    );

    my $key = "key" . int( rand(1024) );
    my $hash = { a => 1 };
    my $exp_props = { n_val => 1, allow_mult => 0 };
    foreach (@buckets) {
        $client->put( $_ => $key => $hash );
        $client->set_bucket_props($_, $exp_props);
    }

    my @props = map { $client->get_bucket_props($_) } @buckets;

    is( scalar @props, scalar @buckets);
    is_deeply($_, $exp_props, "wrong props structure") foreach (@props);
};
