package com.example.sample;

import java.time.Instant;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

public class CloudWatchReporter {
    private final CloudWatchClient cw;
    private final Logger logger = LoggerFactory.getLogger(CloudWatchReporter.class);
    private long maxLag = 0;

    public CloudWatchReporter() {
        cw = CloudWatchClient.builder()
                .build();
    }

    public void report(final long lag) {
        synchronized (this) {
            if (maxLag < lag) {
                maxLag = lag;
            }
        }
    }

    public void start() {

        final Dimension dimension = Dimension.builder()
                .name("ECSServices")
                .value("TestConsumer")
                .build();
        final TimerTask timertask = new TimerTask() {
            @Override
            public void run() {
                final MetricDatum metricDatum;
                synchronized (this) {
                    if (maxLag == 0) {
                        return;
                    }
                    metricDatum = MetricDatum.builder()
                            .metricName("RecordLag")
                            .unit(StandardUnit.MILLISECONDS)
                            .value(Double.valueOf(maxLag))
                            .timestamp(Instant.now())
                            .storageResolution(Integer.valueOf(1)) // High resolution
                            .dimensions(dimension).build();
                    maxLag = 0;

                }
                final PutMetricDataRequest request = PutMetricDataRequest.builder()
                        .namespace("EventHubs")
                        .metricData(metricDatum)
                        .build();
                try {
                    cw.putMetricData(request);
                } catch (final CloudWatchException e) {
                    logger.error(e.getMessage());
                }
            }
        };
        new Timer().scheduleAtFixedRate(timertask, new Date(), 1000);
    }
}
