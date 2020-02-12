package com.github.rhmessaging.qdrshipshape.examples.basic.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Result {
    public int delivered;
    public int accepted;
    public int released;
    public int rejected;
    public String errormsg = "";

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    public static void main(String[] args) {
        Result r = new Result();
        r.accepted = 10;
        r.delivered = 13;
        r.rejected = 1;
        r.released = 2;
        r.errormsg = "Timed out";
        System.out.println(r);
    }
}
