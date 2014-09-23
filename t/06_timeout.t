use strict;
use warnings;
BEGIN {
    unless ( $ENV{RIAK_PBC_HOST} ) {
        require Test::More;
        Test::More::plan(
            skip_all => 'variable RIAK_PBC_HOST is not defined' );
    }
}

use Test::More;
use Test::Exception;
use Riak::Client;

use AnyEvent;

my ( $host, $port ) = split ':', $ENV{RIAK_PBC_HOST};

my $client = Riak::Client->new(
    anyevent_mode => 1,
    host => $host,
    port => $port,
    read_timeout => 0.5,
);

$client->put_raw(bucket => 'foo' => 42);

my $cv = AE::cv;
$client->get_raw(bucket => 'foo', sub {
                     say STDERR $_[0];
                     $cv->send;
                 });
$cv->recv();

ok(1);

done_testing;
