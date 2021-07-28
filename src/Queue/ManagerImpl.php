<?php

/**
 * Defines the class CodeRage\Queue\ManagerImpl
 *
 * File:        CodeRage/Queue/ManagerImpl.php
 * Date:        Wed Dec 25 18:09:34 UTC 2019
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Queue;

use Throwable;
use CodeRage\Access\Session;
use CodeRage\Error;
use CodeRage\Log;
use CodeRage\Util\Args;
use CodeRage\Util\Time;


/**
 * Helper class to give CodeRage\Queue\Task access to private properties of
 * CodeRage\Queue\Manager
 */
final class ManagerImpl {

    public function __destruct()
    {
        $this->manager = null;
    }

    /**
     * Updates the status of a processing job owned by the current processor and
     * increments the number of attempts
     *
     * @param CodeRage\Queue\TaskImpl $task the task
     * @param int $status One of the constants STATUS_XXX
     * @param array $options The options array; supports the following options:
     *     maintainOwnership - true if the current queue processing session
     *       should keep ownership of the job; defaults to true. Has no effect
     *       unless $status is STATUS_PENDING; in other cases, $task's session
     *       ID is always cleared.
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
    public function updateTask(TaskImpl $task, int $status, array $options = [])
    {
        // Process options
        $maintainOwnership =
            Args::checkKey($options, 'maintainOwnership', 'boolean', [
                'default' => true
            ]);
        $error = Args::checkKey($options, 'error', 'CodeRage\Error');
        $errorStatus = Args::checkKey($options, 'errorStatus', 'string');
        $errorMessage = Args::checkKey($options, 'errorMessage', 'string');
        if ( $status == Task::STATUS_SUCCESS &&
             ($error !== null || $errorStatus !== null) )
        {
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        "The options 'error' and 'errorStatus' are " .
                        "incompatible with STATUS_SUCCESS"
                ]);
        }
        if ($error !== null && $errorStatus !== null)
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        "The options 'error' and 'errorStatus' are incompatible"
                ]);
        if (($errorStatus !== null) != ($errorMessage !== null))
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        "The options 'errorStatus' and 'errorMessage' must " .
                        "be specified together"
                ]);
        if ($error !== null) {
            $errorStatus = $error->status();
            $errorMessage = $error->details();
        }

        $enc = null;
        if ($this->log !== null)
            $this->tool->logMessage('Updating task status', [
                'task' => $task->encode(),
                'status' => $status,
                'options' => $options
            ]);

        // Update task
        try {
            $sessionid = $status == Task::STATUS_PENDING &&
                         $options['maintainOwnership'] ?
                $this->session->sessionid() :
                null;
            $completed = $status != Task::STATUS_PENDING ?
                Time::get() :
                null;
            $this->updateTaskStatement()->execute([
                $status, $sessionid, $completed,
                $errorStatus, $errorMessage, $task->id
            ]);
            $task->status = $status;
            $task->attempts = $task->attempts + 1;
            $task->sessionid = $sessionid;
            $task->completed = $completed;
            $task->errorStatus = $errorStatus;
            $task->errorMessage = $errorMessage;
            if ($status == Task::STATUS_FAILURE) {
                $enc = json_encode($task->encode());
                $this->tool->logCritical(
                    "Queue '{$this->queue}: Task failed permanently: $enc"
                );
            }
        } catch (Throwable $e) {
            $enc =
                json_encode([
                    'task' => $task->encode(),
                    'status' => $status,
                    'options' => $options
                ]);
            throw new
                Error([
                    'details' =>
                        "Queue '{$this->queue}': Failed updating task " .
                        "status $enc",
                    'inner' => $e
                ]);
        }

        if ($this->log !== null)
            $this->tool->logMessage('Done updating task status', [
                'task' => $task->encode(),
                'status' => $status,
                'options' => $options
            ]);
    }

    /**
     * Sets the first data item associated with the given task to the given
     * value
     *
     * @param CodeRage\Queue\TaskImpl $task the task
     * @param string $value The value
     */
    public function setData1(TaskImpl $task, string $value): void
    {
        $this->setData1Statement()->execute([$value, $task->id]);
    }

    /**
     * Sets the first data item associated with the given task to the given
     * value
     *
     * @param CodeRage\Queue\TaskImpl $task the task
     * @param string $value The value
     */
    public function setData2(TaskImpl $task, string $value): void
    {
        $this->setData2Statement()->execute([$value, $task->id]);
    }

    /**
     * Sets the first data item associated with the given task to the given
     * value
     *
     * @param CodeRage\Queue\TaskImpl $task the task
     * @param string $value The value
     */
    public function setData3(TaskImpl $task, string $value): void
    {
        $this->setData3Statement()->execute([$value, $task->id]);
    }

    /**
     * Returns a prepared statement with placeholders for creation date,
     * task identifier, the three optional data items associated with a task,
     * the runtime parameters, the expiration date, the maxAttempts value,
     * and the session ID
     *
     * @return CodeRage\Db\Statement
     */
    public function createTaskStatement()
    {
        if ($this->createTaskStatement === null) {
            $this->createTaskStatement =
                $this->tool->db()->prepare(
                    "INSERT INTO [$this->queue]
                     ( CreationDate, taskid, data1, data2, data3,
                       parameters, expires, maxAttempts, attempts,
                       sessionid, status )
                     VALUES (%i, %s, %s, %s, %s, %s, %i, %i, 0, %s, 1)"
                );
        }
        return $this->createTaskStatement;
    }

    /**
     * Returns a prepared statement with placeholders for task identifier and
     * runtime parameters, for loading a task from the queue
     *
     * @return CodeRage\Db\Statement
     */
    public function loadTaskStatement()
    {
        if ($this->loadTaskStatement === null) {
            $this->loadTaskStatement =
                $this->tool->db()->prepare(
                    "SELECT *
                     FROM [$this->queue]
                     WHERE taskid = %s AND
                           parameters = %s"
                );
        }
        return $this->loadTaskStatement;
    }

    /**
     * Returns a prepared statement with placeholders for session ID and task
     * database ID, for updating a task's session ID
     *
     * @return CodeRage\Db\Statement
     */
    public function updateSessionidStatement()
    {
        if ($this->updateSessionidStatement === null) {
            $this->updateSessionidStatement =
                $this->tool->db()->prepare(
                    "UPDATE [$this->queue]
                     SET sessionid = %s
                     WHERE RecordID = %i"
                );
        }
        return $this->updateSessionidStatement;
    }

    /**
     * Returns a prepared statement with placeholders for status, session ID,
     * completion timestamp, error status, error message, and task database ID
     *
     * @return CodeRage\Db\Statement
     */
    public function updateTaskStatement()
    {
        if ($this->updateTaskStatement === null) {
            $this->updateTaskStatement =
                $this->tool->db()->prepare(
                    "UPDATE [$this->queue]
                     SET status = %i,
                         attempts = attempts + 1,
                         sessionid = %s,
                         completed = %i,
                         errorStatus = %s,
                         errorMessage = %s
                     WHERE RecordID = %i"
                );
        }
        return $this->updateTaskStatement;
    }

    /**
     * Returns a prepared statement with placeholders for data3 and task
     * database ID
     *
     * @return CodeRage\Db\Statement
     */
    public function setData1Statement()
    {
        if ($this->setData1Statement === null) {
            $this->setData1Statement =
                $this->tool->db()->prepare(
                    "UPDATE [$this->queue]
                     SET data1 = %s
                     WHERE RecordID = %i"
                );
        }
        return $this->setData1Statement;
    }

    /**
     * Returns a prepared statement with placeholders for data2 and task
     * database ID
     *
     * @return CodeRage\Db\Statement
     */
    public function setData2Statement()
    {
        if ($this->setData2Statement === null) {
            $this->setData2Statement =
                $this->tool->db()->prepare(
                    "UPDATE [$this->queue]
                     SET data2 = %s
                     WHERE RecordID = %i"
                );
        }
        return $this->setData2Statement;
    }

    /**
     * Returns a prepared statement with placeholders for data3 and task
     * database ID
     *
     * @return CodeRage\Db\Statement
     */
    public function setData3Statement()
    {
        if ($this->setData3Statement === null) {
            $this->setData3Statement =
                $this->tool->db()->prepare(
                    "UPDATE [$this->queue]
                     SET data3 = %s
                     WHERE RecordID = %i"
                );
        }
        return $this->setData3Statement;
    }

    /**
     * Returns a prepared statement with a placeholder for task database ID, for
     * clearing a task's session ID and incrementing its attempts
     *
     * @return CodeRage\Db\Statement
     */
    public function updateAttemptsStatement()
    {
        if ($this->updateAttemptsStatement === null) {
            $this->updateAttemptsStatement =
                $this->tool->db()->prepare(
                    "UPDATE [$this->queue]
                     SET sessionid = NULL,
                         attempts = attempts + 1
                     WHERE RecordID = %i"
                );
        }
        return $this->updateAttemptsStatement;
    }


    /**
     * Returns a prepared statement with a placeholders for task database ID,
     * for updating a task's status
     *
     * @return CodeRage\Db\Statement
     */
    public function markFailedStatement()
    {
        if ($this->markFailedStatement === null) {
            $this->markFailedStatement =
                $this->tool->db()->prepare(
                    "UPDATE [$this->queue]
                     SET status = 1
                     WHERE RecordID = %i"
                );
        }
        return $this->markFailedStatement;
    }

    /**
     * Returns a prepared statement with a placeholder for task database ID, for
     * deleting a task
     *
     * @return CodeRage\Db\Statement
     */
    public function deleteTaskStatement()
    {
        if ($this->deleteTaskStatement === null) {
            $this->deleteTaskStatement =
                $this->tool->db()->prepare(
                    "DELETE FROM [$this->queue]
                     WHERE RecordID = %i"
                );
        }
        return $this->deleteTaskStatement;
    }

    /**
     * The containing queue manager
     *
     * @var CodeRage\Tool\Tool
     */
    public $manager;

    /**
     * The queue processing tool
     *
     * @var CodeRage\Tool\Tool
     */
    public $tool;

    /**
     * The name of the database table containing the queue
     *
     * @var string
     */
    public $queue;

    /**
     * The runtime parameters
     *
     * @var string
     */
    public $parameters;

    /**
     * The the number of times newly created tasks may fail before being
     * assigned the status STATUS_FAILURE, if no value is passed to createTask()
     *
     * @var int
     */
    public $maxAttempts;

    /**
     * The lifetime of newly crated tasks, in seconds, if no value is passed to
     * createTask()
     *
     * @var int
     */
    public $lifetime;

    /**
     * The queue processing session
     *
     * @var CodeRage\Access\Session
     */
    public $session;

    /**
     * The log, if queue logging is enabled
     *
     * @var CodeRage\Log
     */
    public $log;

    /**
     * @var CodeRage\Db\Statement
     */
    private $createTaskStatement;

    /**
     * @var CodeRage\Db\Statement
     */
    private $loadTaskStatement;

    /**
     * @var CodeRage\Db\Statement
     */
    private $updateSessionidStatement;

    /**
     * @var CodeRage\Db\Statement
     */
    private $updateTaskStatement;

    /**
     * @var CodeRage\Db\Statement
     */
    private $setData1Statement;

    /**
     * @var CodeRage\Db\Statement
     */
    private $setData2Statement;

    /**
     * @var CodeRage\Db\Statement
     */
    private $setData3Statement;

    /**
     * @var CodeRage\Db\Statement
     */
    private $updateAttemptsStatement;

    /**
     * @var CodeRage\Db\Statement
     */
    private $markFailedStatement;

    /**
     * @var CodeRage\Db\Statement
     */
    private $deleteTaskStatement;
}
