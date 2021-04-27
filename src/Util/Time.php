<?php

/**
 * Defines the class CodeRage\Util\Time
 *
 * File:        CodeRage/Util/Time.php
 * Date:        Wed Feb  8 05:11:16 UTC 2017
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2017 CounselNow
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Util;

use CodeRage\Error;
use CodeRage\Util\Args;


/**
 * Provides access to the current time, adjusted by a stored offset, as well
 * as utility methods for manipulating dates and times
 */
final class Time {

    private CONST CREATE_OPTIONS =
        [ 'timezone' => 1, 'year' => 1, 'day' => 1, 'month' => 1, 'hour' => 1,
          'minute' => 1, 'second' => 1, 'timestamp' => 1, 'modify' => 1,
          'format' => 1 ];

    /**
     * Returns the current time, as a UNIX timestamp, adjusted by a stored
     * offset
     *
     * @return int The current time, as a UNIX timestamp, plus the stored
     *   offset. Initially the stored offset is the value of the configuration
     *   variable "coderage.util.time.offset", if it is set, and 0 otherwise.
     *   The stored offset may be modified by calling the method set().
     */
    public static function get()
    {
        if (self::$offset === null) {
            $config = \CodeRage\Config::current();
            $offset = $config->getProperty('coderage.util.time.offset', 0);
            self::$offset = ctype_digit($offset) ? (int) $offset : 0;
        }
        return time() + self::$offset;
    }

    /**
     * Sets the stored offset to the difference between the given timestamp and
     * the current time
     *
     * @param int $time A UNIX timestamp
     */
    public static function set($time)
    {
        Args::check($time, 'int', 'timestamp');
        self::$offset = $time - time();
    }

    /**
     * Resets the stored offset to zero
     */
    public static function reset() { self::$offset = 0; }

    /**
     * Returns the return of calling the built-in function time(). Used to
     * document cases where adjusted timestamps should not be used.
     *
     * @return int
     */
    public static function real() { return time(); }

    /**
     * Returns an instance of DateTime or a formatted timestamp string with the
     * specified properties. If a timestamp is supplied, the time of day may be
     * customized with the "hour", "minute", and "second" options; if no year
     * or timestamp is supplied, the current timestamp will be used, as returned
     * by get()
     *
     * @param array $options The options array; supports the following options:
     *     timezone - A time zone identifier suitable for passing to
     *       the DateTimeZone constructor, a UTC offset in seconds, or an
     *       instance of DateTimeZone; defaults to "UTC"
     *     year - The year (optional)
     *     month - The month (optional)
     *     day - The day of the month (optional)
     *     hour - The hour (optional)
     *     minute - The minute (optional)
     *     second - The second (optional)
     *     timestamp - A UNIX timestamp; defaults to the current time if
     *       no year, month, and day are specified
     *     modify - A string to be passed to DateTime::modify() after
     *       construction (optional)
     *     format - A format string suitable for passing to date() (optional)
     *   The following constraints apply:
     *     - The options "year", "month", and "day" must all be supplied if any
     *       one of them is supplied
     *     - If one of the options "minute" or "second" is supplied, all the
     *       less fine-grained time of day options must also be supplied; e.g.,
     *       if "minute" is supplied, "hour" must also be supplied
     *     - "timestamp" may not be supplied together with the options "year",
     *       "month", and "day"
     * @return mixed An instance of DateTIme or a string
     */
    public static function createDateTime(array $options = [])
    {
        foreach (array_keys($options) as $n)
            if (!array_key_exists($n, self::CREATE_OPTIONS))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' => "Unsupported option: $n"
                    ]);
        $timezone =
            Args::checkKey($options, 'timezone', 'int|string|DateTimeZone', [
                'default' => 'UTC'
            ]);
        $year = Args::checkKey($options, 'year', 'int');
        $month = Args::checkKey($options, 'month', 'int');
        $day = Args::checkKey($options, 'day', 'int');
        $hour = Args::checkKey($options, 'hour', 'int');
        $minute = Args::checkKey($options, 'minute', 'int');
        $second = Args::checkKey($options, 'second', 'int');
        $timestamp = Args::checkKey($options, 'timestamp', 'int');
        $modify = Args::checkKey($options, 'modify', 'string');
        $format = Args::checkKey($options, 'format', 'string');
        if ($year !== null && ($year < 1 || $year > 9999))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Invalid year: $year"
                ]);
        if ($month !== null && ($month < 1 || $month > 12))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Invalid month: $month"
                ]);
        if ($day !== null && ($day < 1 || $day > 31))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Invalid day: $day"
                ]);
        if ($hour !== null && ($hour < 0 || $hour > 23))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Invalid hour: $hour"
                ]);
        if ($minute !== null && ($minute < 0 || $minute > 59))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Invalid minute: $minute"
                ]);
        if ($second !== null && ($second < 0 || $second > 59))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Invalid second: $second"
                ]);

        // Check consistency
        if ( ($year !== null) != ($month !== null) ||
             ($year !== null) != ($day !== null) )
        {
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        "The options 'year', 'month', and 'day' must be " .
                        "specified together"
                ]);
        }
        if ($year !== null && !checkdate($month, $day, $year)) {
            $date = sprintf('%04d-%02d-%02d', $year, $month, $day);
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Invalid date: $date"
                ]);
        }
        if ($minute !== null && $hour === null)
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' => "Missing 'hour'"
                ]);
        if ($second !== null && $minute === null)
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' => "Missing 'minute'"
                ]);
        if ($timestamp !== null && $year !== null)
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        "The options 'timestamp' and 'year' are incompatible"
                ]);

        // Process time zone
        if (is_int($timezone))
            $timezone = self::formatUtcOffset($timezone);
        if (is_string($timezone))
            $timezone = new \DateTimeZone($timezone);

        // Construct DateTime
        $date = new \CodeRage\Util\DateTime(null, $timezone);
        if ($year !== null) {
            $date->setDate($year, $month, $day);
            if ($hour === null)
                $hour = $minute = $second = 0;
        } else {
            if ($timestamp === null)
                $timestamp = Time::get();
            $date->setTimestamp($timestamp);
        }
        if ($hour !== null)
            $date->setTime($hour, $minute, $second);

        // Modify
        if ($modify !== null) {
            $handler = new ErrorHandler;
            $result = $handler->_modify($date, $modify);
            if ($result === false || $handler->errno())
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' =>
                            $handler->formatError('Failed modifying date')
                    ]);
        }

        // Format
        if ($format !== null)
            $date = $date->format($format);

        return $date;
    }

    /**
     * Returns a timezone offset string suitable for passing as the time_zone
     * constructor parameter to DateTime, e.g., "+0630"
     *
     * @param int $offset A UTC offset, in seconds
     * @return string
     */
    public static function formatUtcOffset(int $seconds) : string
    {
        $sign = $seconds >= 0 ? '+' : '-';
        $abs = abs($seconds);
        $hours = floor($abs / 3600);
        $minutes = floor(($abs - 3600 * $hours) / 60);
        return sprintf('%s%02d%02d', $sign, $hours, $minutes);
    }

    /**
     * Executes the given callable one for each day in the the given date range
     *
     * @param string $begin The first day in the range, in the format yyyy-mm-dd
     * @param string $end The last day in the range, in the format yyyy-mm-dd
     * @param callable $action A callable taking a single date argument, in the
     *   format yyyy-mm-dd
     */
    public static function foreachDay(string $begin, string $end,
        callable $action) : void
    {
        Args::check($begin, 'date', 'begin date');
        Args::check($end, 'date', 'end date');
        if ($begin > $end)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'Begin date is later than end date'
                ]);
        $date = new DateTime($begin);
        while (($current = $date->format('Y-m-d')) <= $end) {
            $action($current);
            $date->modify('+1 day');
        }
    }

    /**
     * @var int
     */
    private static $offset;
}
