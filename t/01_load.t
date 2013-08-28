use Test::More tests => 1;

my $ok;
END { BAIL_OUT "Could not load all modules" unless $ok }
use Riak::Client;
use Riak::Client::PBC;
use Riak::Client::Connector;
use Riak::Client::Driver;
use Riak::Client::Timeout;
use Riak::Client::Timeout::Alarm;
use Riak::Client::Timeout::Select;
use Riak::Client::Timeout::SelectOnRead;
use Riak::Client::Timeout::TimeOut;
use Riak::Client::Util;

ok 1, 'All modules loaded successfully';
$ok = 1;