package com.spark.sql.udf;

import org.apache.spark.sql.api.java.UDF2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Returns Number of Days between given date and system date.
 * Format is CalculateDays("Date", "DateFormat")
 */
public class CalculateDaysUDF implements UDF2<String, String, Long>
{
    @Override
    public Long call(String sDate, String dateFormat) throws Exception {
        SimpleDateFormat fmt = new SimpleDateFormat(dateFormat);
        Date d1 = fmt.parse(sDate);
        Date d2 = new Date();
        long l = d2.getTime() - d1.getTime();

        return TimeUnit.DAYS.convert(l, TimeUnit.MILLISECONDS);
    }
}
