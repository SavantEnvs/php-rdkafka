// Low-level consumer: RdKafka\Consumer + ConsumerTopic consumeStart/consume/consumeBatch/
// consumeCallback/offsetStore/consumeQueueStart with full-range 64-bit fuzz arguments
// (reaches the consumeBatch batch_size overflow fixed in d493f28). No broker is
// configured, so calls return locally; timeouts are kept at 0-3 ms.
try {
    $conf = new RdKafka\Conf();
    $conf->set('log_level', '0');
    $conf->set('enable.auto.commit', (u8() & 1) ? 'true' : 'false');
    $conf->set('enable.auto.offset.store', 'false');
    $rk = new RdKafka\Consumer($conf);
    $tc = new RdKafka\TopicConf();
    $tc->set('auto.offset.reset', (u8() & 1) ? 'earliest' : 'latest');
    $topic = $rk->newTopic(str() ?: 't', $tc);
    $queue = $rk->newQueue();
// batch_size: full 64-bit range except values whose 8-byte-per-slot array would be a
// legitimate multi-GB request (2^20 .. 2^61): the harness runs with USE_ZEND_ALLOC=0, so
// PHP's memory_limit, which refuses those, is not enforced. Values >= 2^61 (where
// batch_size * sizeof(ptr) overflows, d493f28) and negatives are passed unchanged.
function batch_size() { $b = i64(); return ($b > (1 << 20) && $b < (1 << 61)) ? ($b & 0xfffff) : $b; }
    $n = 0;
    while (more() && $n++ < 24) {
        try {
            switch (u8() % 9) {
            case 0: $topic->consumeStart(i64(), i64()); break;
            case 1: $topic->consumeStop(i64()); break;
            case 2: $m = $topic->consume(i64(), u8() % 4); if ($m) { $m->errstr(); $m->headers; } break;
            case 3:
                $r = $topic->consumeBatch(i64(), u8() % 4, batch_size());
                foreach ($r as $m) { $m->errstr(); }
                break;
            case 4: $topic->consumeCallback(i64(), u8() % 4, function ($m) { $m->errstr(); }); break;
            case 5: $topic->offsetStore(i64(), i64()); break;
            case 6: $topic->consumeQueueStart(i64(), i64(), $queue); break;
            case 7: $m = $queue->consume(u8() % 4); if ($m) { $m->errstr(); } break;
            case 8: $rk->poll(0); $rk->getOutQLen(); break;
            }
        } catch (Throwable $t) {}
    }
} catch (Throwable $t) {}
