package com.jum.utils.result;


import java.util.HashMap;
import java.util.Map;

public class ResultMap<T> extends HashMap {
    public static final String CODE = "code";
    public static final String ERROR_CODE = "errorCode";
    public static final String DATA = "data";
    public static final String RECORD = "record";
    public static final String MESSAGE = "message";
    public static final int ERROR = 0;
    public static final int SUCCESS = 1;

    public ResultMap(T t) {
        this.put("code", Integer.valueOf(1));
        if (t instanceof Result) {
            Result result = (Result)t;
            if (!result.isSuccess()) {
                this.put("code", Integer.valueOf(0));
                this.put("errorCode", result.getResultCode());
                this.put("message", result.getMessage());
            }

            if (result.getModel() == null && result.getModels() != null && result.getModels().size() > 0) {
                this.put("data", result.getModels());
                this.put("record", result.getTotalRecord());
            } else {
                this.put("data", result.getModel());
            }
        } else if (t instanceof Map) {
            this.putAll((Map)t);
        } else {
            this.put("data", t);
        }

    }

    /** @deprecated */
    @Deprecated
    public ResultMap(boolean success, String errorCode, String message) {
        this.put("code", success ? 1 : 0);
        this.put("errorCode", errorCode);
        this.put("message", message);
    }

    /** @deprecated */
    @Deprecated
    public ResultMap(boolean success, String errorCode, String message, T t) {
        this(t);
        this.put("code", success ? 1 : 0);
        this.put("errorCode", errorCode);
        this.put("message", message);
    }

    /** @deprecated */
    @Deprecated
    public ResultMap(boolean success, String errorCode, String message, int record, T t) {
        this(success, errorCode, message, t);
        this.put("record", record);
    }

    public ResultMap(String errorCode, String message) {
        this.put("code", Integer.valueOf(0));
        this.put("errorCode", errorCode);
        this.put("message", message);
    }

    public ResultMap(String errorCode, String message, T t) {
        this(t);
        this.put("code", Integer.valueOf(0));
        this.put("errorCode", errorCode);
        this.put("message", message);
    }

    public ResultMap(String errorCode, String message, int record, T t) {
        this(errorCode, message, t);
        this.put("record", record);
    }

    public ResultMap() {
        this.put("code", Integer.valueOf(1));
    }

    public ResultMap(boolean success) {
        this.put("code", success ? 1 : 0);
    }

    public void setSuccess(boolean success) {
        this.put("code", success ? 1 : 0);
    }

    public void setErrorCode(String errorCode) {
        this.put("errorCode", errorCode);
    }

    public void setMessage(String message) {
        this.put("message", message);
    }

    public void setData(T t) {
        this.put("data", t);
    }
}
