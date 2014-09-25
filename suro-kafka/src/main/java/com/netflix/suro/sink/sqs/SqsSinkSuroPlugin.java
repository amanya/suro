package com.netflix.suro.sink.sqs;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.sink.notice.NoNotice;
import com.netflix.suro.sink.notice.QueueNotice;

public class SqsSinkSuroPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(SqsSink.TYPE, SqsSink.class);

        this.addNoticeType(NoNotice.TYPE, NoNotice.class);
        this.addNoticeType(QueueNotice.TYPE, QueueNotice.class);
    }
}
