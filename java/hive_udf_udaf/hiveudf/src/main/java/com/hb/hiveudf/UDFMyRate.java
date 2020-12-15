package com.hb.hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class UDFMyRate extends UDF {

    private double FINACIAL_PRECISION = 0.00000001;
    private double FINACIAL_MAX_ITERATIONS = 128;

    public double evaluate(double npr, double pmt, double pv, double fv, int type) {
        double rate = 0.1;
        double y;
        double f = 0.0;
        if (Math.abs(rate) < FINACIAL_PRECISION) {
            y = pv * (1 + npr * rate) + pmt * (1 + rate * type) * npr + fv;
        } else {
            f = Math.exp(npr * Math.log(1 + rate));
            y = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;
        }
        double y0 = pv + pmt * npr + fv;
        double y1 = pv * f + pmt * (1 + rate + type) * (f - 1) + fv;
        int i = 0;
        double x0 = 0.0;
        double x1 = rate;
        while ((Math.abs(y0 - y1) > FINACIAL_PRECISION) && (i < FINACIAL_MAX_ITERATIONS)) {
            rate = (y1 * x0 - y0 * x1) / (y1 - y0);
            x0 = x1;
            x1 = rate;
            if (Math.abs(rate) < FINACIAL_PRECISION) {
                y = pv * (1 + npr * rate) + pmt * (1 + rate * type) * npr + fv;
            } else {
                f = Math.exp(npr * Math.log(1 + rate));
                y = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;
            }
            y0 = y1;
            y1 = y;
            i++;
        }
        return rate;
    }

    public static void main(String[] args) {
//        // TODO Auto-generated method stub
        UDFMyRate evaluator_1 = new UDFMyRate();


        double rate = evaluator_1.evaluate(24, 500, -10000, 0, 1);

        System.out.println(rate);

    }

}
