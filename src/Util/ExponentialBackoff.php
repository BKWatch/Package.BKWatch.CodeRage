<?php

/**
 * Defines the class CodeRage\Util\ExponentialBackoff
 *
 * File:        CodeRage/Util/ExponentialBackoff.php
 * Date:        Mon May 14 19:48:28 UTC 2018
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2018 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Util;

use Exception;
use Throwable;
use CodeRage\Error;
use CodeRage\Log;
use CodeRage\Util\Args;


/**
 * Executes a user-supplied operation, repeating it several times as necessary
 * using an exponential backoff strategy
 */
final class ExponentialBackoff {

    /**
     * @var int
     */
    const DEFAULT_ATTEMPTS = 5;

    /**
     * @var float
     */
    const DEFAULT_SLEEP = 1.0;

    /**
     * @var float
     */
    const DEFAULT_MULTIPLIER = 2.0;

    /**
     * Constructs a CodeRage\Util\ExponentialBackoff
     *
     * @param array $options The options array; supports the following options:
     *     attempts - The maximum number times to attempt to execute the
     *       operation
     *     sleep - The number of seconds to sleep after the first failed
     *       attempt; The length of subsquent sleep intervals is determined
     *       using the options 'sleep' and 'multipler'
     *     multiplier - The amount by which the sleep interval should be
     *       increased after each failed attempt
     */
    public function __construct($options = [])
    {
        Args::checkKey($options, 'attempts', 'int', [
            'default' => self::DEFAULT_ATTEMPTS
        ]);
        if ($options['attempts'] <= 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid 'attempts' option; expected an integer " .
                        "greater than or equal to 1; found " .
                        $options['attempts']
                ]);
        Args::checkKey($options, 'sleep', 'float', [
            'default' => self::DEFAULT_SLEEP
        ]);
        if ($options['sleep'] <= 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid 'sleep' option; expected a positive " .
                        "numeric value; found " . $options['sleep']
                ]);
        Args::checkKey($options, 'multiplier', 'float', [
            'default' => self::DEFAULT_MULTIPLIER
        ]);
        if ($options['multiplier'] < 1)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid 'multiplier' option; expected a numeric " .
                        "value greater than or equal to 1; found " .
                        $options['multiplier']
                ]);
        $this->attempts = $options['attempts'];
        $this->sleep = $options['sleep'];
        $this->multiplier = $options['multiplier'];
    }

    /**
     * Executes the given operation, repeating it as necessaru, using an
     * exponential backoff strategy
     *
     * @param callable $operation The operation to execute
     * @param callable $errorHandler A callable taking a single argument
     *   to be called each time $operation throws an exception, returning true
     *   if $operation should be attempted again and false if the exception
     *   should be re-thrown
     * @param mixed $message A participial phrase describing the operation,
     *   as a string or as a callable returning a string
     * @param CodeRage\Log $log A log for writing messages; defaults to the
     *   current log
     */
    public function execute($operation, $errorHandler, $message,
        $log = null)
    {
        if ($log === null)
            $log = Log::current();
        $sleep = $this->sleep;
        $attempts = $this->attempts;
        $result = $attempt = $error = null;
        for ($attempt = 1; $attempt <= $attempts; ++$attempt) {
            if ($stream = $log->getStream(Log::DEBUG)) {
                if ($attempt == 1) {
                    if (is_callable($message)) {
                        $message = $message();
                    }
                    $message = ucfirst($message);
                }
                $stream->write("$message: attempt $attempt out of $attempts");
            }
            try {
                $result = $operation();
                $error = null;
                break;
            } catch (Throwable $e) {
                $error = $e;
                if ($errorHandler($e) && $attempt < $attempts) {
                    $reason = Error::encode($error, true);
                    $log->logMessage(
                        "Attempt $attempt failed; trying again ($reason)"
                    );
                    $this->sleep($sleep);
                    $sleep *= $this->multiplier;
                } else {
                    break;
                }
            }
        }
        if ($error !== null)
            throw $error;
        $level = $attempt > 1 ? Log::INFO : Log::DEBUG;
        if ($stream = $log->getStream($level))
            $stream->write("Success on attempt $attempt");
        return $result;
    }

    /**
     * Sleeps for the given number of seconds
     *
     * @param float $seconds
     */
    private function sleep($seconds)
    {
        usleep($seconds * 1000000);
    }

    /**
     * @var int
     */
    private $attempts;

    /**
     * @var float
     */
    private $sleep;

    /**
     * @var float
     */
    private $multiplier;
}
