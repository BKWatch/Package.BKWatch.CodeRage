<?php

/**
 * Defines the class CodeRage\Util\Random
 *
 * File:        CodeRage/Util/Args_.php
 * Date:        Thu Sep 17 02:04:09 UTC 2020
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Util;

use CodeRage\Error;

/**
 * Container for static methods used to validate and process function arguments
 */
final class Random {

    /**
     * Alphabet consisting of lowercase and uppercase letters
     *
     * @var string
     */
    public const ALPHA = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

    /**
     * Alphabet consisting of lowercase and uppercase letters and digits
     *
     * @var string
     */
    public const ALNUM =
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

    /**
     * Alphabet consisting of lowercase letters
     *
     * @var string
     */
    public const LOWER = 'abcdefghijklmnopqrstuvwxyz';

    /**
     * Alphabet consisting of uppercase letters
     *
     * @var string
     */
    public const UPPER = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

    /**
     * Alphabet consisting of digits
     *
     * @var string
     */
    public const DIGIT = '01234567890';

    /**
     * Alphabet consisting of lowercase hex digits
     *
     * @var string
     */
    public const XDIGIT = '01234567890abcdef';

    /**
     * Returns a random string of the given length, using the specified alphabet
     *
     * @param int $length The length
     * @param string $alphabet The alphabet, as a string of single-byte
     *   character
     */
    public static function string_(int $length, string $alphabet = self::ALNUM)
        : string
    {
        $max = strlen($alphabet) - 1;
        $value = '';
        for ($z = $length - 1; $z != -1; --$z)
            $value .= $alphabet[random_int(0, $max)];
        return $value;
    }

    public static function __callStatic($method, $args)
    {
        return $method == 'string' ? self::string_(...$args) : null;
    }
}
