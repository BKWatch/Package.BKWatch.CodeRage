<?php

/**
 * Defines the class CodeRage\Queue\Task
 *
 * File:        CodeRage/Queue/Task.php
 * Date:        Wed Dec 25 18:27:37 UTC 2019
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Queue;

use CodeRage\Error;
use CodeRage\Util\Args;


/**
 * Represents a task in a queue
 */
final class Task {

    /**
     * @var int
     */
    const STATUS_SUCCESS = 0;

    /**
     * @var int
     */
    const STATUS_PENDING = 1;

    /**
     * @var int
     */
    const STATUS_FAILURE = 2;

    /**
     * Special value for use with the "parameters" option to the
     * CodeRage\Queue\Manager constructor
     *
     * @var float
     */
    const NO_PARAMS = INF;

    /**
     * Constructs a CodeRage\Queue\Task
     *
     * @param CodeRage\Queue\ManagerImpl $manager The queue manager
     *   implementation
     * @param array $row An associative array representing a row in the queue
     */
    public function __construct(ManagerImpl $manager, array $row)
    {
        Args::check($row, 'map', 'queue row');
        $this->impl = new TaskImpl;
        $this->manager = $manager;
        $this->impl->id =
            Args::checkIntKey($row, 'RecordID', ['required' => true]);
        $this->impl->created =
            Args::checkIntKey($row, 'CreationDate', ['required' => true]);
        $this->impl->taskid =
            Args::checkKey($row, 'taskid', 'string', [
                'label' => 'task identifier',
                'required' => true
            ]);
        $this->impl->parameters =
            Args::checkKey($row, 'parameters', 'string', ['required' => true]);
        foreach (
            [ 'data1', 'data2', 'data3', 'sessionid',
              'errorStatus', 'errorMessage']
            as $n )
        {
            $this->impl->$n = Args::checkKey($row, $n, 'string');
        }
        if ( ($this->impl->errorStatus !== null) !=
             ($this->impl->errorMessage !== null) )
        {
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        "The options 'errorStatus' and 'errorMessage' must " .
                        "be specified together"
                ]);
        }
        foreach (['expires', 'attempts', 'status'] as $n)
            $this->impl->$n = Args::checkIntKey($row, $n, ['required' => true]);
        if ($this->impl->attempts < 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'Attempts must be non-negative'
                ]);
        if ( $this->impl->status < self::STATUS_SUCCESS ||
             $this->impl->status > self::STATUS_FAILURE )
        {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Unsupported status: {$this->impl->status}"
                ]);
        }
        $this->impl->maxAttempts = Args::checkIntKey($row, 'maxAttempts');
        if ($this->impl->maxAttempts !== null && $this->impl->maxAttempts < 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'Attempts must be non-negative'
                ]);
        $this->impl->completed = Args::checkIntKey($row, 'completed');
    }

    /**
     * Returns the opaque task identifier
     *
     * @return string
     */
    public function taskid()
    {
        return $this->impl->taskid;
    }

    /**
     * Returns the first data item associated with the task, if any
     *
     * @return string
     */
    public function data1()
    {
        return $this->impl->data1;
    }

    /**
     * Sets the first data item associated with the task to the given value
     *
     * @param string $value The value
     */
    public function setData1(string $value): void
    {
        $this->manager->setData1($this->impl, $value);
    }

    /**
     * Returns the second data item associated with the task, if any
     *
     * @return string
     */
    public function data2()
    {
        return $this->impl->data2;
    }

    /**
     * Sets the second data item associated with the task to the given value
     *
     * @param string $value The value
     */
    public function setData2(string $value): void
    {
        $this->manager->setData2($this->impl, $value);
    }

    /**
     * Returns the third data item associated with the task, if any
     *
     * @return string
     */
    public function data3()
    {
        return $this->impl->data3;
    }

    /**
     * Sets the third data item associated with the task to the given value
     *
     * @param string $value The value
     */
    public function setData3(string $value): void
    {
        $this->manager->setData3($this->impl, $value);
    }

    /**
     * Returns the runtime parameters
     *
     * @return string
     */
    public function parameters()
    {
        return $this->impl->parameters;
    }

    /**
     * Returns the creation date, as a UNIX timestamp
     *
     * @return int
     */
    public function created()
    {
        return $this->impl->created;
    }

    /**
     * Returns the expiration date, as a UNIX timestamp
     *
     * @return int
     */
    public function expires()
    {
        return $this->impl->expires;
    }

    /**
     * Returns the number of times this task may fail before being assigned the
     * status STATUS_FAILURE
     *
     * @return int
     */
    public function maxAttempts()
    {
        return $this->impl->maxAttempts;
    }

    /**
     * Returns the number of times this task has been attempted
     *
     * @return int
     */
    public function attempts()
    {
        return $this->impl->attempts;
    }


    /**
     * Returns the alphanumeric session ID, if any
     *
     * @return string
     */
    public function sessionid()
    {
        return $this->impl->sessionid;
    }

    /**
     * Returns the task status, as the value of one of the STATUS_XXX constants
     *
     * @return int
     */
    public function status()
    {
        return $this->impl->status;
    }

    /**
     * Returns the time this task's status was set to STATUS_SUCCESS or
     * STATUS_FAILURE, as a UNIX timestamp
     *
     * @return int
     */
    public function completed()
    {
        return $this->impl->completed;
    }

    /**
     * Returns the status of the most recent exception thrown during task
     * processing, if any
     *
     * @return string
     */
    public function errorStatus()
    {
        return $this->impl->errorStatus;
    }

    /**
     * Returns the details of the most recent exception thrown during task
     * processing, if any
     *
     * @return string
     */
    public function errorMessage()
    {
        return $this->impl->errorMessage;
    }

    /**
     * Updates the status of a processing job owned by the current processor and
     * increments the number of attempts
     *
     * @param mixed $task An opaque task identifier
     * @param int $status One of the constants STATUS_XXX
     * @param array $options The options array; supports the following options:
     *     maintainOwnership - true if the current processor should keep
     *       ownership of the job; defaults to true if $status is STATUS_PENDING
     *       and to false otherwise
     *     error - An instance of CodeRage\Error used to set the value of the
     *       columns "errorStatus" and "errorMessage" (optional)
     *     errorStatus - The status of the last exception thrown during task
     *       processing (optional)
     *     errorMessage - The details of the last exception thrown during task
     *       processing (optional)
     *   At most one of "error" and "errorStatus" may be specified; neither is
     *   allowed if $status is STATUS_SUCCESS. The options "errorStatus" and
     *   "errorMessage" mst be specified together.
     */
    public function update($status, $options = [])
    {
        $this->manager->updateTask($this->impl, $status, $options);
    }

    /**
     * Deletes this task
     *
     * @throws CodeRage\Error
     */
    public function delete()
    {
        $this->manager->deleteTaskStatement()->execute([$this->impl->id]);
    }

    /**
     * Encodes this task as an instance of stdClass for use in logging
     *
     * @return stdClass
     */
    public function encode()
    {
        return $this->impl->encode();
    }

    /**
     * Hook for use with CodeRage\Util\NativeDataEncoder
     *
     * @param CodeRage\Util\NativeDataEncoder $encoder
     * @return stdClass
     */
    public function nativeDataEncode(\CodeRage\Util\NativeDataEncoder $encoder)
    {
        return $this->impl->encode();
    }

    public function __toString()
    {
        return json_encode($this->encode());
    }

    /**
     * The queue manager implementation
     *
     * @var CodeRage\Queue\ManagerImpl
     */
    public $manager;

    /**
     * The task implementation
     *
     * @var CodeRage\Queue\TaskImpl
     */
    public $impl;
}
