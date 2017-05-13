package com.gokhanozg;

import java.io.Serializable;

/**
 * Created by mephala on 5/13/17.
 */
public class Image implements Serializable {
    int pixels[][] = new int[28][28];
    String label;


    public int[][] getPixels() {
        return pixels;
    }

    public void setPixels(int[][] pixels) {
        this.pixels = pixels;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }


}
