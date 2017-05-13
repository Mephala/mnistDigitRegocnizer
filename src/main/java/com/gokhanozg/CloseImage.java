package com.gokhanozg;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Created by mephala on 5/13/17.
 */
public class CloseImage implements Serializable {
    String label;
    BigDecimal error;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public BigDecimal getError() {
        return error;
    }

    public void setError(BigDecimal error) {
        this.error = error;
    }
}
