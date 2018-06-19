package utils;

/**
 * Created by daiquanyi on 2017/2/27.
 */
public class LogUtil {

    public static String formatErrorInfo(Exception e) {
        StringBuilder stringBuilder = new StringBuilder();
        StackTraceElement[] stackTrace = e.getStackTrace();
        stringBuilder.append(e);
        stringBuilder.append("\r\n");
        for (int i = 0; i < stackTrace.length; i++) {
            stringBuilder.append(stackTrace[i]);
            if (stackTrace.length > i+1) {
                stringBuilder.append("\r\n");
            }
        }

        return stringBuilder.toString();
    }
}
