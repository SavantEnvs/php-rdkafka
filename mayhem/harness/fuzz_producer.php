// Producer path against librdkafka's in-process mock cluster (test.mock.num.brokers, loopback
// only, no external broker): ProducerTopic::produce/producev with fuzz-chosen partition,
// msgflags, payload, key, headers (incl. non-string values), timestamp and opaque, then
// poll/flush/purge so delivery reports run through the dr_msg callback into RdKafka\Message.
try {
    $conf = new RdKafka\Conf();
    $conf->set('log_level', '0');
    $conf->set('test.mock.num.brokers', '1');
    $conf->set('linger.ms', '0');
    $conf->set('message.timeout.ms', '200');
    $conf->setDrMsgCb(function ($rk, $m) { $m->errstr(); $m->headers; $m->opaque; });
    $conf->setErrorCb(function ($rk, $err, $reason) {});
    $rk = new RdKafka\Producer($conf);
    $topic = $rk->newTopic(str() ?: 't');
    $n = 0;
    while (more() && $n++ < 16) {
        try {
            switch (u8() % 6) {
            case 0:
                $topic->produce(i32(), u8() % 8, (u8() & 1) ? str() : null, (u8() & 1) ? str() : null, (u8() & 1) ? str() : null);
                break;
            case 1:
                $h = [];
                $hn = u8() % 5;
                for ($i = 0; $i < $hn; $i++) {
                    switch (u8() % 4) { case 0: $h[str()] = str(); break; case 1: $h[str()] = i64(); break; case 2: $h[str()] = null; break; default: $h[] = str(); }
                }
                $topic->producev(i32(), u8() % 8, (u8() & 1) ? str() : null, (u8() & 1) ? str() : null, $h, i64(), (u8() & 1) ? str() : null);
                break;
            case 2: $rk->poll(u8() % 4); break;
            case 3: $rk->flush(u8() % 20); break;
            case 4: $rk->purge(i64()); break;
            case 5: $rk->getOutQLen(); break;
            }
        } catch (Throwable $t) {}
    }
    $rk->flush(50);
    $rk->purge(RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
    $rk->poll(0);
} catch (Throwable $t) {}
