package test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by root on 2016/7/25.
 */
public class TestTime8 {

    public static void main(String[] args) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = dateFormat.parse("2013-6-2 16:00:00");
            calendar.setTime(date);
            calendar.add(Calendar.HOUR_OF_DAY, 7);
            calendar.add(Calendar.MINUTE, 59);
            calendar.add(Calendar.SECOND, 59);
            String str = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")).format(calendar.getTime());
            System.out.println(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
