package naming;

import java.util.concurrent.CompletableFuture;

public class AccessRequest {
    private boolean exclusive;
    // This is a channel in GO
    CompletableFuture<Boolean> done = new CompletableFuture<>();

    public AccessRequest(boolean exclusive) {
        this.exclusive = exclusive;       
    }

    public boolean isExclusive() {
        return exclusive;
    }
}