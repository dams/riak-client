use Test::More tests => 6;
use Test::Exception;
use Riak::Client;

dies_ok { Riak::Client->new } "should ask for port and host";
dies_ok { Riak::Client->new( host => '127.0.0.1' ) } "should ask for port";
dies_ok { Riak::Client->new( port => 8087 ) } "should ask for host";

subtest "new and default attrs values" => sub {
    my $client = new_ok(
        'Riak::Client' => [
            host    => '127.0.0.1',
            port    => 9087,
            driver  => undef
        ],
        "a new client"
    );
    is( $client->timeout, 0.5, "default timeout should be 0.5" );
    is( $client->r,       2,   "default r  should be 2" );
    is( $client->w,       2,   "default w  should be 2" );
    is( $client->dw,      2,   "default dw should be 2" );
    ok( $client->timeout_provider, 'Riak::Client::Timeout::Select' );
};

subtest "new and other attrs values" => sub {
    my $client = new_ok(
        'Riak::Client' => [
            host             => '127.0.0.1',
            port             => 9087,
            timeout          => 0.2,
            r                => 1,
            w                => 1,
            dw               => 1,
            driver           => undef,
            in_timeout       => 2,
            out_timeout      => 4,
            timeout_provider => 'Riak::Client::Timeout::TimeOut'
        ],
        "a new client"
    );
    is( $client->timeout, 0.2, "timeout should be 0.2" );
    is( $client->r,       1,   "r  should be 1" );
    is( $client->w,       1,   "w  should be 1" );
    is( $client->dw,      1,   "dw should be 1" );
    ok( $client->timeout_provider, 'Riak::Client::Timeout::TimeOut' );
    is( $client->in_timeout,  2);
    is( $client->out_timeout, 4);
};

subtest "should be a riak::light instance" => sub {
    isa_ok(
        Riak::Client->new( host => 'host', port => 9999, driver => undef ),
        'Riak::Client'
    );
  }
