// Client-level RdKafka methods on a Producer bound to librdkafka's in-process mock cluster
// (test.mock.num.brokers, loopback only): getMetadata + walking the Metadata\Collection
// iterators out of order (metadata*.c), queryWatermarkOffsets, offsetsForTimes,
// pausePartitions/resumePartitions, addBrokers, setLogLevel, oauthbearerSetToken/
// oauthbearerSetTokenFailure (oauthbearer.c, token + extensions array), and the
// transactional methods with KafkaErrorException creation (rdkafka.c, kafka_error_exception.c).
function walk($c) {
    $k = 0;
    switch (u8() % 4) {
    case 0: foreach ($c as $x) { $k++; } break;
    case 1: $c->rewind(); while ($c->valid()) { $c->current(); $c->key(); $c->next(); } break;
    default: $c->count(); $c->next(); $c->current(); $c->key(); $c->valid(); $c->rewind(); break;
    }
    return $c;
}
function tplist() {
    $l = [];
    $n = u8() % 4;
    for ($i = 0; $i < $n; $i++) { $l[] = new RdKafka\TopicPartition(str() ?: 't', i32(), i64()); }
    return $l;
}
try {
    $conf = new RdKafka\Conf();
    $conf->set('log_level', '0');
    $conf->set('test.mock.num.brokers', '1');
    $conf->set('socket.timeout.ms', '100');
    if (u8() & 1) { $conf->set('transactional.id', str() ?: 'x'); }
    if (u8() & 1) { $conf->set('security.protocol', 'SASL_PLAINTEXT'); $conf->set('sasl.mechanisms', 'OAUTHBEARER'); }
    $conf->setErrorCb(function ($rk, $err, $reason) {});
    $rk = new RdKafka\Producer($conf);
    $n = 0;
    while (more() && $n++ < 16) {
        try {
            switch (u8() % 12) {
            case 0:
                $md = $rk->getMetadata((bool)(u8() & 1), (u8() & 1) ? $rk->newTopic(str() ?: 't') : null, u8() % 8);
                $md->getOrigBrokerId(); $md->getOrigBrokerName();
                foreach (walk($md->getBrokers()) as $b) { $b->getId(); $b->getHost(); $b->getPort(); }
                foreach (walk($md->getTopics()) as $t) {
                    $t->getTopic(); $t->getErr();
                    foreach (walk($t->getPartitions()) as $pa) { $pa->getId(); $pa->getErr(); $pa->getLeader(); walk($pa->getReplicas()); walk($pa->getIsrs()); }
                }
                break;
            case 1: $lo = 0; $hi = 0; $rk->queryWatermarkOffsets(str() ?: 't', i32(), $lo, $hi, u8() % 4); break;
            case 2: $rk->offsetsForTimes(tplist(), u8() % 4); break;
            case 3: $rk->pausePartitions(tplist()); break;
            case 4: $rk->resumePartitions(tplist()); break;
            case 5: $rk->addBrokers(str()); break;
            case 6:
                $ext = [];
                $en = u8() % 3;
                for ($i = 0; $i < $en; $i++) { $ext[str()] = (u8() & 1) ? str() : i64(); }
                $rk->oauthbearerSetToken(str(), (u8() & 1) ? i64() : (string)i64(), str(), $ext);
                break;
            case 7: $rk->oauthbearerSetTokenFailure(str()); break;
            case 8: $rk->initTransactions(u8() % 4); break;
            case 9: $rk->beginTransaction(); break;
            case 10: (u8() & 1) ? $rk->commitTransaction(u8() % 4) : $rk->abortTransaction(u8() % 4); break;
            case 11: $rk->poll(0); $rk->getOutQLen(); $rk->setLogLevel(u8() % 8); break;
            }
        } catch (Throwable $t) {}
    }
    $rk->purge(RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
} catch (Throwable $t) {}
