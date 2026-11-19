// RdKafka\Conf / TopicConf: set/get/dump with fuzz-chosen key/value pairs, then
// construct a Producer or Consumer from it. Keys that start threads with network
// listeners, load files or plugins, or run commands are not passed (they reach the
// filesystem/OS, not the extension).
function allowed($k) {
    foreach (['mock', 'location', 'plugin', 'kinit', 'keytab', 'ssl', 'sasl', 'path', 'file', 'engine', 'debug', 'oidc', 'interceptor', 'bootstrap', 'broker.list'] as $b) {
        if (stripos($k, $b) !== false) return false;
    }
    return true;
}
try {
    $conf = new RdKafka\Conf();
    $tc = new RdKafka\TopicConf();
    $conf->set('log_level', '0');
    $n = 0;
    while (more() && $n++ < 24) {
        $op = u8() % 6;
        try {
            if ($op == 0 || $op == 1) {
                $k = str(); $v = str();
                if (!allowed($k)) continue;
                if ($op == 0) { $conf->set($k, $v); $conf->get($k); } else { $tc->set($k, $v); $tc->get($k); }
            } elseif ($op == 2) {
                $tc->setPartitioner(i64());
            } elseif ($op == 3) {
                $conf->dump(); $tc->dump();
            } elseif ($op == 4) {
                @$conf->setDefaultTopicConf($tc);
            } else {
                $conf->get(str());
            }
        } catch (Throwable $t) {}
    }
    $conf->set('log_level', '0');
    $k = (u8() & 1) ? new RdKafka\Producer($conf) : new RdKafka\Consumer($conf);
    $t = $k->newTopic(str(), $tc);
    $t->getName();
    $k->poll(0);
} catch (Throwable $t) {}
