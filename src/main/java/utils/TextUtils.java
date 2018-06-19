package utils;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by daiquanyi on 2017/3/8.
 */
public class TextUtils {

    private static Logger logger = Logger.getLogger(TextUtils.class);

    public static boolean isEmpty(CharSequence s) {
        return s == null ? true : s.length() == 0;
    }

    public static boolean isBlank(CharSequence s) {
        if (s == null) {
            return true;
        } else if (s.equals("null")) {
            return true;
        } else if(s.length() == 0) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean containsBlanks(CharSequence s) {
        if (s == null) {
            return false;
        } else {
            for (int i = 0; i < s.length(); ++i) {
                if (Character.isWhitespace(s.charAt(i))) {
                    return true;
                }
            }

            return false;
        }
    }

    public static String[] removeDuplicate(String[] arrayStr) {
        TreeSet<String> tr = new TreeSet<>();
        for (int i = 0; i < arrayStr.length; i++) {
            tr.add(arrayStr[i]);
        }
        String[] s2 = new String[tr.size()];
        System.out.println("=====处理后======");
        for (int i = 0; i < s2.length; i++) {
            s2[i] = tr.pollFirst();//从TreeSet中取出元素重新赋给数组
        }
        return s2;
    }

    public static String formatDate(String time) {
        if (time == null) {
            return time;
        }
        SimpleDateFormat oldFormat = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        SimpleDateFormat newFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String replace = time.replaceAll("[^\\d:]", " ");
        try {
            Date date = oldFormat.parse(replace);
            String format = newFormat.format(date);
            return format;
        } catch (ParseException e) {
            logger.error(LogUtil.formatErrorInfo(e));
        }

        return null;
    }

    public static boolean isContainChinese(String str) {

        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(str);
        if (m.find()) {
            return true;
        }
        return false;
    }
}
