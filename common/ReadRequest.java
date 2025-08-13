package common;

public class ReadRequest {
    public String path;
    public long offset;
    public long length;

    public ReadRequest(String path, long offset, long length) {
        this.path = path;
        this.offset = offset;
        this.length = length;
    }
    
    @Override
    public String toString() {
        return "ReadRequest: " + "path = <" + path + "> offset = <" + offset + "> length = <" + length + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof ReadRequest)) return false;
        ReadRequest readRequest = (ReadRequest) obj;
        return this.path.equals(readRequest.path) && this.offset == readRequest.offset && this.length == readRequest.length;
    }
}
