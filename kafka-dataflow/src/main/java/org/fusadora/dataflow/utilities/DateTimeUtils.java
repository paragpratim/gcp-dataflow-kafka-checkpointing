package org.fusadora.dataflow.utilities;

import org.fusadora.dataflow.exception.StaticUtilityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class DateTimeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtils.class);

    private static final ThreadLocal<DateFormat> dateTimeFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            df.setLenient(Boolean.FALSE);
            return df;
        }
    };
    private static final ThreadLocal<DateFormat> dateTimeFormatWithoutZone = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            df.setLenient(Boolean.FALSE);
            return df;
        }
    };
    private static final ThreadLocal<DateFormat> dateTimeFormatBq = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            df.setLenient(Boolean.FALSE);
            return df;
        }
    };
    private static final ThreadLocal<DateFormat> dateTimeFormatSimple = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            df.setLenient(Boolean.FALSE);
            return df;
        }
    };

    public static String convertBqTimeToSimpleDateTimeString(String bqDateTime, long offsetMilli) {
        try {
            Date date = dateTimeFormatBq.get().parse(bqDateTime);
            return dateTimeFormatSimple.get().format(new Date(date.getTime() + offsetMilli));
        } catch (ParseException e) {
            throw new StaticUtilityException("Unable to parse Bigquery DateTime [" + bqDateTime + "]");
        }
    }

    public static String convertSimpleDateTimeToBqDateTimeString(String simpleDateTime, long offsetMilli) {
        try {
            Date date = dateTimeFormatSimple.get().parse(simpleDateTime);
            return dateTimeFormatBq.get().format(new Date(date.getTime() + offsetMilli));
        } catch (ParseException e) {
            throw new StaticUtilityException("Unable to parse Bigquery DateTime [" + simpleDateTime + "]");
        }
    }

    public static String getSimpleDateTimeString(Date dateTime) {
        return dateTimeFormatSimple.get().format(dateTime);
    }

}
