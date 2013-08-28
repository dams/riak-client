use Test::More tests => 2;
use FindBin qw($Bin);
use lib "$Bin/tlib";
use TestTimeout qw(test_timeout test_normal_wait);

test_timeout('Riak::Client::Timeout::SelectOnRead');
test_normal_wait('Riak::Client::Timeout::SelectOnRead');
