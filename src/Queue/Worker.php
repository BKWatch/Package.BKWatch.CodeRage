<?php

/**
 * Defines the class CodeRage\Queue\Worker
 *
 * File:        CodeRage/Queue/Worker.phps
 * Date:        Fri Jan  3 16:30:13 UTC 2020
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2018 CounselNow
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Queue;

use Throwable;
use CodeRage\Config;
use CodeRage\Error;
use CodeRage\File;
use CodeRage\Sys\BasicHandle;
use CodeRage\Util\ErrorHandler;
use CodeRage\Util\ExponentialBackoff;
use CodeRage\Util\Time;

/**
 * Implements the batch processor mode "worker"
 */
final class Worker extends BasicHandle {

    /**
     * @var string
     */
    private const RUN_TOOL_PATH = 'bin/run-tool';

    /**
     * @var int
     */
    private const OPEN_ATTEMPTS = 5;

    /**
     * @var float
     */
    private const OPEN_SLEEP = 0.5;

    /**
     * @var float
     */
    private const OPEN_MULTIPLIER = 2.0;

    /**
     * Constructs a Util\TaskProcessor\Slave
     *
     * @param CodeRage\Queue\Manager$manager An instance of CodeRage\Queue\Manager
     *   that owns the tasks to be performed by the worker
     * @param array $options The task processor options, encoded as strings
     */
    public function __construct(Manager $manager, array $options)
    {
        parent::__construct();
        $this->manager = $manager;
        $this->options = $options;
    }

    /**
     * Returns the task-processing session ID of this worker process
     *
     * @return string
     */
    public function sessionid()
    {
        return $this->manager->sessionid();
    }

    /**
     * Starts this worker
     */
    public function start()
    {
        // Construct command
        $tool =  Config::projectRoot() . '/' . self::RUN_TOOL_PATH;
        $class = $this->options['taskClass'];
        $command =
            escapeshellarg($tool) . ' -c ' . escapeshellarg($class) .
            ' --param taskSessionid=' . $this->sessionid();
        foreach ($this->options as $n => $v) {
            if ($v !== null) {
                if (is_bool($v)) {
                    $v = (int) $v;
                }
                $command .= ' --param ' . escapeshellarg("$n=$v");
            }
        }
        if ($session = \CodeRage\Access\Session::current())
            $command .= ' --session sessionid=' . $this->session()->sessionid();
        if (isset($this->options['taskDebug']))
            $command .= ' -d ' . $this->options['taskDebug'];
        $this->outputFile = File::temp();
        $this->errorFile = File::temp();
        $spec =
            [
                0 => ['pipe', 'r'],
                1 => ['file', $this->outputFile, 'a'],
                2 => ['file', $this->errorFile, 'a']
            ];
        $pipes = null;
        $backoff =
            new ExponentialBackoff([
                    'attempts' => self::OPEN_ATTEMPTS,
                    'sleep' => self::OPEN_SLEEP,
                    'multipler' => self::OPEN_MULTIPLIER
                ]);
        $this->proc =
            $backoff->execute(
                function() use($command, $spec, &$pipes)
                {
                    $proc = proc_open($command, $spec, $pipes);
                    if ($proc === false)
                        throw new
                            Error([
                                'status' => 'INTERNAL_ERROR',
                                'details' =>
                                    "Failed starting worker (command '$command')"
                            ]);
                    return $proc;
                },
                function ($e) { return true; },
                'starting worker',
                $this->log()
            );
    }

    /**
     * Returns true if this worker process has timed out
     */
    public function timedOut()
    {
        $expires =
            $this->db()->fetchValue(
                'SELECT expires
                 FROM AccessSession
                 WHERE sessionid = %s',
                $this->sessionid()
            );
        return $expires < Time::real();
    }

    /**
     * Returns true if this worker process has termianted
     *
     * @return boolean
     */
    public function terminated()
    {
        $handler = new ErrorHandler;
        $info = $handler->_proc_get_status($this->proc);
        if ($info === false || $handler->errno()) {
            $this->cleanup();
            throw new
                Error([
                    'status' => 'INTERNAL_ERROR',
                    'details' =>
                        $handler->formatError(
                            'Failed querying worker ' .
                            $this->sessionid()
                        )
                ]);
        }
        $this->info = $info;
        return !$info['running'];
    }

    /**
     * Returns the output of the worker process, as an associative array
     * reference, and throws and exception if the process failed or output a
     * malformed result
     */
    public function result()
    {
        $sessionid = $this->sessionid();
        $status = $this->info['exitcode'];
        if ($status != 0) {
            $error = is_file($this->errorFile) && is_readable($this->errorFile) ?
                file_get_contents($this->errorFile) :
                '';
            $this->cleanup();
            throw new
                Error([
                    'status' => 'INTERNAL_ERROR',
                    'details' =>
                        "Slave $sessionid failed with exit status $status (" .
                        "$error)"
                ]);
        }
        try {
            File::checkReadable($this->errorFile);
        } catch (Throwable $e) {
            $this->cleanup();
            throw new
                Error([
                    'status' => 'INTERNAL_ERROR',
                    'details' => "Failed reading output of worker $sessionid",
                    'inner' => $e
                ]);
        }
        $output = file_get_contents($this->outputFile);
        $result = json_decode($output);
        if ($result === null) {
            $this->cleanup();
            throw new
                Error([
                    'status' => 'INTERNAL_ERROR',
                    'details' =>
                        "JSON decoding error: expected tool output; found " .
                        "'$output'"
                ]);
        }
        if (!$result instanceof \stdClass) {
            $this->cleanup();
            throw new
                Error([
                    'status' => 'INTERNAL_ERROR',
                    'details' =>
                        "Malformed tool output: expected object; found " .
                        Error::formatValue($result)
                ]);
        }
        $this->cleanup();
        if ($result->status == 'SUCCESS' && isset($result->result)) {
            return $result->result;
        } else {
            throw new Error((array) $result);
        }
    }

    /**
     * Closes process handles and deletes temporary files
     */
    private function cleanup()
    {
        $handler = new ErrorHandler;
        if (is_resource($this->proc)) {
            $result = $handler->_proc_close($this->proc);
            if ($handler->errno()) {
                $this->log()->logError(
                    $handler->formatError(
                        'Failed cleaning up worker ' . $this->sessionid()
                    )
                );
            }
            $this->proc = null;
        }
        if (is_string($this->outputFile) && file_exists($this->outputFile)) {
            $handler->_unlink($this->outputFile);
            $this->outputFile = null;
        }
        if (is_string($this->errorFile) && file_exists($this->errorFile)) {
            $handler->_unlink($this->errorFile);
            $this->errorFile = null;
        }
        $this->info = null;
    }

    /**
     * @var CodeRage\Queue\Manager
     */
    private $manager;

    /**
     * @var array
     */
    private $options;

    /**
     * @var resource
     */
    private $proc;

    /**
     * @var string
     */
    private $outFile;

    /**
     * @var string
     */
    private $errFile;

    /**
     * @var array
     */
    private $info;
}
