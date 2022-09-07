package be.twofold.crab;

import java.util.concurrent.*;

final class ProgressMonitor {

    private static final long TimeoutNanos = TimeUnit.SECONDS.toNanos(1);

    private long lastUpdate;
    private int lastCount;
    private int count;

    ProgressMonitor() {
        initialize();
    }

    void initialize() {
        lastUpdate = System.nanoTime();
    }

    int incrementCount() {
        count++;
        long nanoTime = System.nanoTime();
        if (nanoTime - lastUpdate >= TimeoutNanos) {
            print();
            lastCount = count;
            lastUpdate = nanoTime;
        }
        return count;
    }

    void print() {
        System.out.println("Saved " + count + " items (" + (count - lastCount) + " items/s)");
    }

}
