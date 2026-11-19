// Value objects and module functions: TopicPartition, ConsumerGroupMetadata,
// KafkaErrorException, rd_kafka_err2str/err2name/errno2err/offset_tail/get_err_descs.
$n = 0;
while (more() && $n++ < 32) {
    try {
        switch (u8() % 8) {
        case 0:
            $tp = new RdKafka\TopicPartition(str(), i64(), i64());
            $tp->setTopic(str()); $tp->setPartition(i64()); $tp->setOffset(i64());
            $tp->getTopic(); $tp->getPartition(); $tp->getOffset(); $tp->getErr(); $tp->getMetadata();
            var_dump($tp); print_r($tp, true);
            break;
        case 1:
            $tp = new RdKafka\TopicPartition(str(), i64(), i64(), u8() & 1 ? str() : null);
            var_dump($tp);
            break;
        case 2:
            $f = u8();
            $g = new RdKafka\ConsumerGroupMetadata(str(), $f & 1 ? i64() : -1, $f & 2 ? str() : "", $f & 4 ? str() : null);
            if (method_exists($g, 'getGroupId')) { $g->getGroupId(); $g->getGenerationId(); $g->getMemberId(); $g->getGroupInstanceId(); }
            var_dump($g); clone $g;
            break;
        case 3:
            $e = new RdKafka\KafkaErrorException(str(), i64(), str(), (bool)(u8() & 1), (bool)(u8() & 1), (bool)(u8() & 1));
            $e->getErrorString(); $e->isFatal(); $e->isRetriable(); $e->transactionRequiresAbort();
            break;
        case 4:
            $c = i64(); rd_kafka_err2str($c); rd_kafka_err2name($c);
            break;
        case 5:
            @rd_kafka_errno2err(i64()); rd_kafka_offset_tail(i64());
            break;
        case 6:
            rd_kafka_get_err_descs(); rd_kafka_thread_cnt(); @rd_kafka_errno();
            break;
        case 7:
            $m = new RdKafka\Message(); $m->err = i64(); $m->payload = str(); $m->errstr(); $m->headers; $m->getOpaque();
            break;
        }
    } catch (Throwable $t) {}
}
