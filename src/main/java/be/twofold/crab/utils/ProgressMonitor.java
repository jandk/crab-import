package be.twofold.crab.utils;

import java.util.concurrent.*;

public final class ProgressMonitor {

    private static final long TimeoutNanos = TimeUnit.SECONDS.toNanos(1);

    private long lastUpdate;
    private int lastCount;
    private int count;

    public ProgressMonitor() {
        initialize();
    }

    void initialize() {
        lastUpdate = System.nanoTime();
    }

    public int incrementCount() {
        count++;
        long nanoTime = System.nanoTime();
        if (nanoTime - lastUpdate >= TimeoutNanos) {
            print();
            lastCount = count;
            lastUpdate = nanoTime;
        }
        return count;
    }

    public void print() {
        System.out.println("Saved " + count + " items (" + (count - lastCount) + " items/s)");
    }

}
