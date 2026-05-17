package com.example.multipipelineetl.common;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class NasaLogParser {
    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\S+)\\s+\\S+\\s+\\S+\\s+\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):\\d{2}:\\d{2}\\s+[+-]\\d{4}]\\s+\"\\S+\\s+([^\\s]+)\\s+\\S+\"\\s+(\\d{3})\\s+(\\S+)$"
    );
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH);

    private NasaLogParser() {
    }

    public static Optional<ParsedLogRecord> parse(String line) {
        Matcher matcher = LOG_PATTERN.matcher(line);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        String host = matcher.group(1);
        String day = matcher.group(2);
        String month = matcher.group(3);
        String year = matcher.group(4);
        int hour = Integer.parseInt(matcher.group(5));
        String resourcePath = matcher.group(6);
        int statusCode = Integer.parseInt(matcher.group(7));
        String bytesToken = matcher.group(8);
        long bytes = "-".equals(bytesToken) ? 0L : Long.parseLong(bytesToken);
        LocalDate logDate = LocalDate.parse(day + "-" + month + "-" + year, DATE_FORMATTER);
        return Optional.of(new ParsedLogRecord(host, logDate, hour, resourcePath, statusCode, bytes));
    }
}

