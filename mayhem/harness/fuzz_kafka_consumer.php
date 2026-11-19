// High-level consumer: RdKafka\KafkaConsumer against librdkafka's in-process mock cluster
// (test.mock.num.brokers, loopback only). Fuzz bytes build TopicPartition lists (topic,
// partition, offset, metadata) that go through the array <-> rd_kafka_topic_partition_list
// conversion in topic_partition.c, then drive assign/incrementalAssign/subscribe/consume/
// commit/getCommittedOffsets/offsetsForTimes/queryWatermarkOffsets/pause/resume/
// getMetadata/getAssignment/getSubscription/close in kafka_consumer.c. Timeouts are 0-3 ms.
function tplist() {
    $l = [];
    $n = u8() % 4;
    for ($i = 0; $i < $n; $i++) {
        $tp = new RdKafka\TopicPartition((u8() & 1) ? (str() ?: 't') : 't', i32(), i64());
        if (u8() & 1) { $tp->setMetadata(str()); }
        $l[] = $tp;
    }
    return $l;
}
try {
    $conf = new RdKafka\Conf();
    $conf->set('log_level', '0');
    $conf->set('test.mock.num.brokers', '1');
    $conf->set('group.id', str() ?: 'g');
    $conf->set('enable.auto.commit', (u8() & 1) ? 'true' : 'false');
    $conf->set('session.timeout.ms', '6000');
    $conf->set('socket.timeout.ms', '100');
    $conf->setErrorCb(function ($rk, $err, $reason) {});
    $conf->setOffsetCommitCb(function ($rk, $err, $parts) {});
    $rk = new RdKafka\KafkaConsumer($conf);
    $n = 0;
    while (more() && $n++ < 16) {
        try {
            switch (u8() % 16) {
            case 0: $rk->assign((u8() & 1) ? tplist() : null); break;
            case 1: $rk->incrementalAssign(tplist()); break;
            case 2: $rk->incrementalUnassign(tplist()); break;
            case 3: $rk->subscribe([str() ?: 't']); break;
            case 4: $rk->unsubscribe(); break;
            case 5: $m = $rk->consume(u8() % 4); if ($m) { $m->errstr(); $m->headers; $m->getTimestampType(); } break;
            case 6: (u8() & 1) ? $rk->commit(tplist()) : $rk->commit(); break;
            case 7: $rk->commitAsync(tplist()); break;
            case 8: $rk->getCommittedOffsets(tplist(), u8() % 4); break;
            case 9: $rk->getOffsetPositions(tplist()); break;
            case 10: $rk->offsetsForTimes(tplist(), u8() % 4); break;
            case 11: $lo = 0; $hi = 0; $rk->queryWatermarkOffsets(str() ?: 't', i32(), $lo, $hi, u8() % 4); break;
            case 12: $rk->pausePartitions(tplist()); $rk->resumePartitions(tplist()); break;
            case 13: $rk->getAssignment(); $rk->getSubscription(); break;
            case 14: $md = $rk->getMetadata((bool)(u8() & 1), null, u8() % 4); foreach ($md->getTopics() as $t) { $t->getTopic(); } break;
            case 15: $rk->storeOffsets(tplist()); break;
            }
        } catch (Throwable $t) {}
    }
    $rk->close();
} catch (Throwable $t) {}
