package twitter;

import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
@RequiredArgsConstructor
public class TwitterProducerThread implements Runnable{
    private final BlockingQueue<String> blockingQueue;
    private final String bearerToken;
    private final Map<String, String> rules;
    private final long ms;

    private final AtomicBoolean ab = new AtomicBoolean(false);

    @Override
    public void run() {
        ab.set(true);
        try {
            FilteredStreamApiV2.setupRules(bearerToken, rules);
            FilteredStreamApiV2.connectStreamToBQ(bearerToken, blockingQueue, ms, ab);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        ab.set(false);
    }
}
