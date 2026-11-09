package PortfolioProject;

public class ConcurrencyCounter {
    private static final int MIN_COUNT = 0;
    private static final int MAX_COUNT = 20;
    private static final int MAX_LABEL_LEN = 32;
    private static final long WAIT_TIMEOUT_MS = 5000L;
    // Private final lock object prevents external lock hijacking (CERT LCK00-J)
    private final Object lock = new Object();
    private int sharedCounter = MIN_COUNT;
    private boolean countUpCompleted = false;
    private boolean abortFlag = false;

    private static String sanitizeLabel(String rawLabel) {

        if (rawLabel == null || rawLabel.isEmpty()) {
            return "Unnamed-Thread";
        }
        int boundedLen = Math.min(rawLabel.length(), MAX_LABEL_LEN);
        StringBuilder clean = new StringBuilder(boundedLen);
        for (int i = 0; i < boundedLen; i++) {
            char c = rawLabel.charAt(i);
            // Allow only printable ASCII characters to block newline/log injection
            if (c >= 32 && c <= 126) {
                clean.append(c);
            }
        }
        return clean.toString();
    }

    // Thread 1 counts up from 0 to 20, then notifies Thread 2.

    public void countUp(String label) {

        final String cleanLabel = sanitizeLabel(label);
        // Pre-sized StringBuilder avoids creating immutable String garbage on every
        // loop
        StringBuilder localLog = new StringBuilder(512);
        synchronized (lock) {

            try {
                sharedCounter = MIN_COUNT;

                while (sharedCounter <= MAX_COUNT) {
                    localLog.append("[").append(cleanLabel).append("] counting up: ")
                            .append(sharedCounter).append(System.lineSeparator());

                    if (sharedCounter == MAX_COUNT) {
                        break;
                    }
                    sharedCounter = Math.addExact(sharedCounter, 1);
                }
                // Flushing buffered output once minimizes I/O blocking inside synchronized
                // block
                System.out.print(localLog.toString());
                countUpCompleted = true;
                lock.notifyAll();
            } catch (ArithmeticException ex) {
                abortFlag = true;
                System.err.println("[ERROR in " + cleanLabel + "]: " + ex.getMessage());
                lock.notifyAll();
            }
        }
    }

    // Thread 2 waits until Thread 1 reaches 20 then counts down to 0.
    public void countDown(String label) {

        final String cleanLabel = sanitizeLabel(label);
        StringBuilder localLog = new StringBuilder(512);
        synchronized (lock) {

            try {
                long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;

                while (!countUpCompleted && !abortFlag) {
                    long remaining = deadline - System.currentTimeMillis();

                    if (remaining <= 0L) {
                        System.err.println("[ERROR in " + cleanLabel + "]: Timed out waiting for Thread 1.");
                        return;
                    }
                    lock.wait(remaining);
                }

                if (abortFlag) {
                    System.err.println("[ERROR in " + cleanLabel + "]: Aborted due to error in Thread 1.");
                    return;
                }

                while (sharedCounter >= MIN_COUNT) {
                    localLog.append("[").append(cleanLabel).append("] counting down: ")
                            .append(sharedCounter).append(System.lineSeparator());

                    if (sharedCounter == MIN_COUNT) {
                        break;
                    }
                    // Math.subtractExact prevents silent 32-bit integer underflow
                    sharedCounter = Math.subtractExact(sharedCounter, 1);
                }

                System.out.print(localLog.toString());

            } catch (InterruptedException ie) {
                // Restores interrupted state
                Thread.currentThread().interrupt();
                System.err.println("[ERROR in " + cleanLabel + "]: Thread was interrupted while waiting.");

            } catch (ArithmeticException ex) {
                System.err.println("[ERROR in " + cleanLabel + "]: " + ex.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        System.out.println("Hello worl...Concurrency Counter (but java this time)!");
        ConcurrencyCounter counter = new ConcurrencyCounter();
        Thread thread2 = new Thread(() -> counter.countDown("Thread 2"), "CountDown-Thread");
        Thread thread1 = new Thread(() -> counter.countUp("Thread 1"), "CountUp-Thread");

        // Start Thread 2 first to prove it safely waits on the monitor lock until
        // Thread 1 reaches 20
        thread2.start();
        thread1.start();

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[FATAL ERROR]: Main thread interrupted while joining worker threads.");
        }

        System.out.println("Tah-dah!");
    }
}