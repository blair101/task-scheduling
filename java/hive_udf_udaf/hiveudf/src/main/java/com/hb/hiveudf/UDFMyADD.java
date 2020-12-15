package com.hb.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class UDFMyADD extends UDF {

    public Integer evaluate(Integer a, Integer b) {
        if (null == a || null == b) {
            return null;
        }
        return a + b;
    }

    public static void main(String[] args) {
//        // TODO Auto-generated method stub
        UDFMyADD evaluator_1 = new UDFMyADD();

        int a = 10, b = 100;

        int sum = evaluator_1.evaluate(a, b);

        System.out.println(sum);

    }

}
