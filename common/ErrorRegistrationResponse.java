package common;

public class ErrorRegistrationResponse {
    public String exception_type;
    public String exception_info;

    public ErrorRegistrationResponse(String exception_type, String exception_info){
        this.exception_type = exception_type;
        this.exception_info = exception_info;
    }
}