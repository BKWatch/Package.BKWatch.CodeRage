<?php

/**
 * Defines the class CodeRage\Queue\Manager
 *
 * File:        CodeRage/Queue/Manager.php
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
 * Creates, updates, and deletes tasks in a queue
 */
final class Manager {

    /**
     * @var int
     */
    const DEFAULT_SESSION_USERID = 1;

    /**
     * @var int
     */
    const DEFAULT_SESSION_LIFETIME = 3600;

    /**
     * @var int
     */
    const DEFAULT_TOUCH_PERIOD = 20;

    /**
     * @var string
     */
    const LOGGING_CONFIG_VARIABLE = 'coderage.queue.logging';

    /**
     * Constructs an instance of CodeRage\Queue\Manager and starts a queue
     * processing session. Supports the following options:
     *
     * @param array $options The options array; supports the following options:
     *     tool - The queue processing tool, as an instance of
     *       CodeRage\Tool\Tool
     *     queue - The name of the database table containing the queue
     *     parameters - The runtime parameters for newly created tasks and of
     *       claimed task, if no "parameters" option is passed to createTask()
     *       or claimTasks(); defaults to an empty string. To avoid setting any
     *       runtime parameters, pass the special value
     *       CodeRage\Queue\Task::NO_PARAMS.
     *     lifetime - The lifetime of newly created tasks, in seconds,
     *       if no "lifetime" options is passed to createTask() (optional)
     *     maxAttempts - The the number of times newly created tasks may fail
     *       before being assigned the status STATUS_FAILURE, if no value is
     *       passed to createTask() (optional)
     *     sessionid - The alphanumeric session ID, if a queue processing
     *       session has already been started by another instance of
     *       CodeRage\Queue\Manager with the same queue name and runtime
     *       parameters (optional)
     *     sessionLifetime - The queue processing session lifetime, in seconds
     *       (optional)
     *     sessionUserid - The ID of the user associatied with the queue
     *       processing session (optional)
     */
    public function __construct(array $options)
    {
        // Process and validate options
        $tool =
            Args::checkKey($options, 'tool', 'CodeRage\\Tool\\Tool', [
                'required' => true
            ]);
        $queue =
            Args::checkKey($options, 'queue', 'string', [
                'required' => true
            ]);
        $parameters = self::processParameters($options);
        $lifetime = Args::checkKey($options, 'lifetime', 'int');
        if ($lifetime !== null && $lifetime < 1)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'Task lifetime must be positive'
                ]);
        $maxAttempts = Args::checkKey($options, 'maxAttempts', 'int');
        if ($maxAttempts !== null && $maxAttempts < 1)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'maxAttempts must be positive'
                ]);
        if ( isset($options['sessionid']) &&
             ( isset($options['sessionLifetime']) ||
               isset($options['sessionUserid']) ) )
        {
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        "The option 'sessionid' is incompatible with the " .
                        "options 'sessionLifetime' and 'sessionUserid"
                ]);
        }
        Args::checkKey($options, 'sessionid', 'string');
        Args::checkKey($options, 'sessionLifetime', 'int', [
            'label' => 'session lifetime',
            'default' => self::DEFAULT_SESSION_LIFETIME
        ]);
        if ($options['sessionLifetime'] < 1)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'Session lifetime must be positive'
                ]);
        Args::checkKey($options, 'sessionUserid', 'int', [
            'label' => 'session user ID',
            'default' => self::DEFAULT_SESSION_USERID
        ]);
        if (isset($options['maxAge']))  // Check for obsolete option
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'Unsupported option: maxAge'
                ]);

        // Initialize instance
        $impl = new ManagerImpl;
        $impl->manager = $this;
        $impl->tool = $tool;
        $impl->queue = $queue;
        $impl->parameters = $parameters;
        $impl->maxAttempts = $maxAttempts;
        $impl->lifetime = $lifetime;
        $config = $impl->tool->config();
        if ($config->getProperty(self::LOGGING_CONFIG_VARIABLE) === '1')
            $impl->log = $impl->tool->log();
        $this->impl = $impl;

        // Initialize session
        $sessionid =  $options['sessionid'] ?? null;
        if ($sessionid !== null) {
            $this->impl->session =
                Session::load([
                    'sessionid' => $sessionid,
                    'touch' => false
                ]);
        } else {

            // Start session and update processing queue
            $this->startSession($options);
            $this->clearSessions();
            $this->markTasksFailed();
        }
    }

    /**
     * Returns the session ID
     *
     * @return string
     */
    public function sessionid(): string
    {
        return $this->impl->session->sessionid();
    }

    /**
     * Returns a newly constructed instance of CodeRage\Queue\Task
     *
     * @param array $row An associative array representing a record in a
     *   database table representing a row in the queue
     */
    public function constructTask(array $row): Task
    {
        return new Task($this->impl, $row);
    }

    /**
     * Creates a task in a queue
     *
     * @param string $taskid The opaque task identifier
     * @param array $options The options array; supports the following options:
     *     data1 - The first data item associatied with the task, if any
     *     data2 - The second data item associatied with the task, if any
     *     data3 - The third data item associatied with the task, if any
     *     parameters - The runtime parameters (optiona); required if this
     *       instance was constructed with the Task::NO_PARAMS as the value of
     *       the "parameters" option
     *     lifetime - The lifetime of the task; required unless a default
     *       lifetime was passed to the CodeRage\Queue\Manager constructor
     *     maxAttempts - The the number of times the task may fail before being
     *       assigned the status STATUS_FAILURE (optional)
     *     takeOwnership - true to attempt to take ownership of the task;
     *       defaults to true
     *     replaceExisting - true to replace an existing task, regardless of
     *       status, with a new task having status STATUS_PENDING and attempts
     *       0; defaults to false
     * @throws CodeRage\Error if a task cannot be created
     */
    public function createTask(string $taskid, array $options = []): void
    {
        // Process options
        $data1 = Args::checkKey($options, 'data1', 'string');
        $data2 = Args::checkKey($options, 'data2', 'string');
        $data3 = Args::checkKey($options, 'data3', 'string');
        $parameters =
            Args::checkKey($options, 'parameters', 'string', [
                'required' => $this->impl->parameters === null,
                'default' => $this->impl->parameters
            ]);
        $lifetime =
            Args::checkKey($options, 'lifetime', 'int', [
                'required' => $this->impl->lifetime === null,
                'default' => $this->impl->lifetime
            ]);
        if ($lifetime < 1)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'Task lifetime must be positive'
                ]);
        $maxAttempts =
            Args::checkKey($options, 'maxAttempts', 'int', [
                'default' => $this->impl->maxAttempts
            ]);
        if ($maxAttempts !== null && $maxAttempts < 1)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'maxAttempts must be positive'
                ]);
        $takeOwnership =
            Args::checkKey($options, 'takeOwnership', 'boolean', [
                'default' => true
            ]);
        $replaceExisting =
            Args::checkKey($options, 'replaceExisting', 'boolean', [
                'default' => false
            ]);

        if ($this->log() !== null) {
            $this->logMessage('Creating task', [
                'taskid' => $taskid,
                'data1' => $data1,
                'data2' => $data2,
                'data3' => $data3,
                'parameters' => $parameters,
                'lifetime' => $lifetime,
                'maxAttempts' => $maxAttempts,
                'takeOwnership' => $takeOwnership,
                'replaceExisting' => $replaceExisting
            ]);
        }

        $db = $this->db();
        $db->beginTransaction();
        $expires = $sessionid = null;
        try {

            // Load existing task, if any
            $old =
                $this->impl->loadTaskStatement()
                     ->execute([$taskid, $parameters])
                     ->fetchArray();
            if ($old !== null) {
                if (!$replaceExisting)
                    throw new
                        Error([
                            'status' => 'OBJECT_EXISTS',
                            'details' => 'Task exists'
                        ]);
                if ( $old['sessionid'] !== null &&
                     $old['sessionid'] !== $this->sessionid() )
                {
                    throw new
                        Error([
                            'status' => 'OBJECT_EXISTS',
                            'details' => 'Task is owned by another processor'
                        ]);
                }
                $this->impl->deleteTaskStatement()->execute([$old['RecordID']]);
            }

            // Create task
            $created = Time::get();
            $expires = $created + $lifetime;
            $sessionid = $takeOwnership ? $this->sessionid() : null;
            $this->impl->createTaskStatement()->execute([
                $created, $taskid, $data1, $data2, $data3,$parameters,
                $expires, $maxAttempts, $sessionid
            ]);
        } catch (Throwable $e) {
            $db->rollback();
            $enc =
                json_encode([
                    'taskid' => $taskid,
                    'data1' => $data1,
                    'data2' => $data2,
                    'data3' => $data3,
                    'parameters' => $parameters,
                    'lifetime' => $lifetime,
                    'maxAttempts' => $maxAttempts,
                    'takeOwnership' => $takeOwnership,
                    'replaceExisting' => $replaceExisting
                ]);
            throw new
                Error([
                    'details' =>
                        "Queue '{$this->impl->queue}': Failed creating task $enc",
                    'inner' => $e
                ]);
        }
        $db->commit();

        if ($this->log() !== null) {
            $this->logMessage('Created task', [
                'taskid' => $taskid,
                'data1' => $data1,
                'data2' => $data2,
                'data3' => $data3,
                'parameters' => $parameters,
                'expires' => $expires,
                'maxAttempts' => $maxAttempts,
                'sessionid' => $sessionid
            ]);
        }
    }

    /**
     * Returns the list of task objects satisfying the given conditions
     *
     * @param array $options The options array; supports the following options:
     *   maxTasks - The maximum number of tasks to claim (optional)
     *   taskid - The task ID (optional)
     *   data1 - The value of the data1 column of the jobs to claim, or a list
     *     of values (optional)
     *   data2 - The value of the data1 column of the jobs to claim, or a list
     *     of values (optional)
     *   data3 - The value of the data1 column of the jobs to claim, or a list
     *     of values (optional)
     *     queue - The name of the database table containing the queue
     *   status - An intergal status or list of intergal statuses; by default,
     *     tasks will be loaded regardless of status
     *   owned - true to return only tasks that are owned by this manager and would
     *     would not be marked as failed by the method markTasksFailed();
     *     defaults to false
     * @return array A list of instances of CodeRGage\Queue\Task
     */
    public function loadTasks(array $options = []): array
    {
        if (isset($options['available']))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Unsupported option: available"
                ]);
        [$sql, $values] = $this->listTasks($options);
        $result = $this->db()->query($sql, $values);
        $tasks = [];
        while ($row = $result->fetchArray())
            $tasks[] = $this->constructTask($row);
        return $tasks;
    }

    /**
     * Takes ownerships of unowned task in the queue, in order of creation
     *
     * @param array $options The options array; supports the following options:
     *   maxTasks - The maximum number of tasks to claim (optional)
     *   data1 - The value of the data1 column of the jobs to claim, or a list
     *     of values (optional)
     *   data2 - The value of the data1 column of the jobs to claim, or a list
     *     of values (optional)
     *   data3 - The value of the data1 column of the jobs to claim, or a list
     *     of values (optional)
     *     queue - The name of the database table containing the queue
     * @return int The number of claimed tasks
     */
    public function claimTasks(array $options = []): int
    {
        foreach (['taskid', 'status', 'available'] as $name)
            if (isset($options[$name]))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' => "Unsupported option: $name"
                    ]);
        $options +=
            [
                'status' => Task::STATUS_PENDING,
                'available' => true
            ];
        [$sql, $values] = $this->listTasks($options);

        if ($this->log() !== null)
            $this->logMessage('Claiming tasks', $options);

        // Claim tasks
        $db = $this->db();
        $db->beginTransaction();
        $tasks = [];
        try {
            $result = $db->query($sql, $values);
            $stmt = $this->impl->updateSessionidStatement();
            while ($task = $result->fetchArray()) {
                if ($this->log() !== null)
                    $this->logMessage('Claiming task', $this->encodeTask($task));
                $stmt->execute([$this->sessionid(), $task['RecordID']]);
                $tasks[] = $task;
            }
        } catch (Throwable $e) {
            $db->rollback();
            $enc =
                json_encode([
                    'queue' => $this->impl->queue,
                    'maxTasks' => $maxTasks,
                    'data1' => $options['data1'],
                    'data2' => $options['data2'],
                    'data3' => $options['data3']
                ]);
            throw new
                Error([
                    'details' =>
                        "Queue '{$this->impl->queue}': Failed claiming tasks $enc",
                    'inner' => $e
                ]);
        }
        $db->commit();

        $count = count($tasks);

        if ($this->log() !== null)
            $this->logMessage("Claimed $count tasks");

        return count($tasks);
    }

    /**
     * Executes the specified callback for each task in the queue owned by
     * the current queue processing session
     *
     * @param array $options The options array; supports the following options:
     *     action - A callable tasking a single task argument, to be invoked for
     *       each task; failure can be indicated by returning false or by
     *       throwing an exception
     *     maintainOwnership - true if the current queue processing session
     *       should keep ownership of the job (optional)
     *     delete - true to delete tasks after processing, instead of updating
     *       their status; defaults to false
     *     touchPeriod - Integer specifying how open to update the expiration
     *       of the current queue processing session (optional)
     *     queryResult - A query result, represented as an instance of
     *       CodeRage\Db\Results, so be used as a source of rows of data from
     *       which to construct task objects, instead of using a standard query
     *       (optional). If this optin is supplied, the "action" callable
     *       will be invoked with the task as its first argument and
     *       an associative array of query results as its second argument.
     * @return array An associative array with the following keys:
     *     total - The number of tasks processed
     *     succeess  The number of tasks processed successfully
     */
    public function processTasks(array $options): array
    {
        // Process options
        $action =
            Args::checkKey($options, 'action', 'callable', [
                'required' => true
            ]);
        $maintainOwnership =
            Args::checkKey($options, 'maintainOwnership', 'boolean');
        $delete =
            Args::checkKey($options, 'delete', 'boolean', [
                'default' => false
            ]);
        $touchPeriod =
            Args::checkKey($options, 'touchPeriod', 'int', [
                'default' => self::DEFAULT_TOUCH_PERIOD
            ]);
        if ($touchPeriod < 1)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'touchPeriod must be positive'
                ]);
        $queryResult =
            Args::checkKey($options, 'queryResult', 'CodeRage\Db\Result');
        $hasCustomResult = isset($options['queryResult']);
        $updateOptions = $maintainOwnership !== null ?
            ['maintainOwnership' => $maintainOwnership] :
            [];

        // Process tasks
        if ($queryResult == null) {
            $sql =
                "SELECT *
                 FROM [{$this->impl->queue}]
                 WHERE sessionid = %s
                 ORDER BY RecordID";
            $queryResult = $this->db()->query($sql, $this->sessionid());
        }
        $total = $success = 0;
        while ($row = $queryResult->fetchArray()) {
            $task = new Task($this->impl, $row);
            if ($hasCustomResult && $task->sessionid() !== $this->sessionid()) {
                throw new
                    Error([
                        'status' => 'STATE_ERROR',
                        'details' =>
                            "Query result contains row for task $task not " .
                            "owned by manager"
                    ]);
            }
            $this->logMessage('Processing task ' . $task->taskid());
            $status = $error = null;
            try {
                $arg2 = $hasCustomResult ? $row : null;
                if ($action($task, $arg2) !== false) {
                    $status = Task::STATUS_SUCCESS;
                    ++$success;
                } else {
                    $status = Task::STATUS_PENDING;
                }
            } catch (Throwable $e) {
                $error = Error::wrap($e);
                $this->impl->tool->logError($error);
                $status = Task::STATUS_PENDING;
            }
            if ($delete) {
                try {
                    $task->delete();
                } catch (Throwable $e) {
                    $this->impl->tool->logError($e);
                }
            } else {
                $task->update($status,
                    $updateOptions + ['error' => $error]
                );
            }
            if (++$total % $touchPeriod == 0)
                $this->touchSession();
        }

        return ['total' => $total, 'success' => $success];
    }

    /**
     * Updates the session expiration timestamp
     */
    public function touchSession(): void
    {
        $this->impl->session->touch();
    }

    /**
     * Updates tasks with status STATUS_PENDING, setting their "sessionid"
     * column to NULL and incrementing their "attempts" columns, for tasks owned
     * by expired sessions
     */
    public function clearSessions(): void
    {
        if ($this->log() !== null)
            $this->logMessage('Clearing sessions');

        // Delete expired sessions
        $db = $this->db();
        $sql = 'DELETE FROM AccessSession WHERE expires < %i';
        $db->query($sql, Time::get());

        // Clear session IDs and increment attempts
        $db->beginTransaction();
        try {
            $sql =
                "SELECT q.*
                 FROM [{$this->impl->queue}] q
                 LEFT JOIN AccessSession s
                   ON s.sessionid = q.sessionid
                 WHERE q.status = 1 AND
                       q.sessionid IS NOT NULL AND
                       s.RecordID IS NULL";
            $result = $db->query($sql);
            $stmt = $this->impl->updateAttemptsStatement();
            while ($task = $result->fetchArray()) {
                if ($this->log() !== null)
                    $this->logMessage(
                        'Clearing session',
                        $this->encodeTask($task)
                    );
                $stmt->execute([$task['RecordID']]);
            }
        } catch (Throwable $e) {
            $db->rollback();
            throw new
                Error([
                    'details' =>
                        "Queue '{$this->impl->queue}': Failed clearing sessions",
                    'inner' => $e
                ]);
        }
        $db->commit();

        if ($this->log() !== null)
            $this->logMessage('Done clearing sessions');
    }

    /**
     * Writes the full contents of the queue to the log, if any
     */
    public function dumpQueue(): void
    {
        if ($this->log() === null)
            return;
        $sql = "SELECT * FROM [{$this->impl->queue}] ORDER BY CreationDate";
        $result = $this->db()->query($sql);
        $tasks = [];
        while ($row = $result->fetchArray())
            $tasks[] = $this->encodeTask($row);
        $this->logMessage('Contents', $tasks);
    }

    /**
     * Helper for loadTasks() and claimTasks()
     *
     * @param array $options The options array; supports the following options:
     *   maxTasks - The maximum number of tasks to claim (optional)
     *   taskid - The task ID (optional)
     *   data1 - The value of the data1 column of the jobs to claim, or a list
     *     of values (optional)
     *   data2 - The value of the data1 column of the jobs to claim, or a list
     *     of values (optional)
     *   data3 - The value of the data1 column of the jobs to claim, or a list
     *     of values (optional)
     *     queue - The name of the database table containing the queue
     *   status - An integral status or list of intergal statuses; by default,
     *     tasks will be loaded regardless of status
     *   available - true to return only tasks that are unowned and would
     *     not be marked as failed by the method markTasksFailed(); defaults to
     *     false
     *   owned - true to return only tasks that are owned by this manager and would
     *     would not be marked as failed by the method markTasksFailed();
     *     defaults to false
     * @return array A pair [$sql, $values] consisting of a SQL query and a list
     *   of parameter values
     */
    private function listTasks(array &$options): array
    {
        // Process options
        $maxTasks = Args::checkKey($options, 'maxTasks', 'int');
        if ($maxTasks !== null && $maxTasks <= 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'maxTasks must be non-negative'
                ]);
        $taskid = Args::checkKey($options, 'taskid', 'string');
        foreach (['data1', 'data2', 'data3'] as $name) {
            $value =
                Args::checkKey($options, $name, 'string|list[string]', [
                    'default' => null
                ]);
            if (is_string($value)) {
                $options[$name] = [$value];
            } elseif ($value !== null && empty($value)) {
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' => "$name must be non-empty"
                    ]);
            }
        }
        $status = Args::checkKey($options, 'status', 'int|list[int]');
        if ($status !== null) {
            if (is_int($status))
                $status = [$status];
            foreach ($status as $s) {
                if ( $s < Task::STATUS_SUCCESS ||
                     $s > Task::STATUS_FAILURE )
                {
                    throw new
                        Error([
                            'status' => 'INVALID_PARAMETER',
                            'details' => "Invalid status: $s"
                        ]);
                }
            }
        }
        $available =
            Args::checkKey($options, 'available', 'boolean', [
                'default' => false
            ]);
        $owned =
            Args::checkKey($options, 'owned', 'boolean', [
                'default' => false
            ]);
        if (isset($options['maxJobs']))  // Check for obsolete option
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'Unsupported option: maxJobs'
                ]);

        // Construct query
        $where = $values = [];
        if ($this->impl->parameters !== null) {
            $where[] = 'parameters = %s';
            $values[] = $this->impl->parameters;
        }
        if ($taskid !== null) {
            $where[] = 'taskid = %s';
            $values[] = $taskid;
        }
        foreach (['data1', 'data2', 'data3'] as $name) {
            if (($value = $options[$name]) !== null) {
                $ph = join(',', array_fill(0, count($value), '%s'));
                $where[] = "$name in ($ph)";
                foreach ($value as $v)
                    $values[] = $v;
            }
        }
        if ($status !== null) {
            $where[] =
                'status IN (' . join(',', array_fill(0, count($status), '%i')) .
                ')';
            foreach ($status as $s)
                $values[] = $s;
        }
        if ($available) {
            $where[] =
                'sessionid IS NULL AND expires >= %i AND
                 (maxAttempts IS NULL OR attempts < maxAttempts)';
            $values[] = Time::get();
        }
        if ($owned) {
            $where[] =
                'sessionid = %s AND expires >= %i AND
                 (maxAttempts IS NULL OR attempts < maxAttempts)';
            $values[] = $this->sessionid();
            $values[] = Time::get();
        }
        $sql =
            "SELECT *
             FROM [{$this->impl->queue}]
             WHERE " . join(' AND ', $where);
        if ($maxTasks !== null) {
            $sql .= ' LIMIT %i';
            $values[] = $maxTasks;
        }

        return [$sql, $values];
    }


    /**
     * Helper method for the constrtuctor, for processing the special value
     * CodeRage\Queue\Task::NO_PARAMS for the option "parameters"
     *
     * @param array $options The options array; supports the following options:
     *     parameters -The runtime parameters for newly created tasks and of
     *       claimed task, if no "parameters" option is passed to createTask()
     *       or claimTasks(); defaults to an empty string. To avoid setting any
     *       runtime parameters, pass the special value
     *       CodeRage\Queue\Task::NO_PARAMS.
     * @eturn string The parameters, if any
     */
    private static function processParameters(array &$options): string
    {
        $parameters =
            Args::checkKey($options, 'parameters', 'float|string', [
                'default' => ''
            ]);
        if (is_float($parameters)) {
            if ($parameters === Task::NO_PARAMS) {
                $parameters = $options['parameters'] = null;
            } else {

                // Throw an exception
                Args::check($parameters, 'string', 'parameters');
            }
        }
        return $parameters;
    }

    /**
     * Starts a new task processing session
     *
     * @params array $options The optins array; supports the following options:
     *     sessionLifetime - The queue processing session lifetime, in seconds
     *     sessionUserid - The ID of the user associatied with the queue
     *       processing session
     */
    private function startSession(array $options): void
    {
        $this->impl->session =
            Session::create([
                'userid' => $options['sessionUserid'],
                'lifetime' => $options['sessionLifetime']
            ]);
    }

    /**
     * Deletes expired tasks from the queue
     */
    private function deleteExpiredTasks(): void
    {
        if ($this->log() !== null)
            $this->logMessage('Deleting expired tasks');

        $db = $this->db();
        $db->beginTransaction();
        try {
            $sql =
                "SELECT *
                 FROM [{$this->impl->queue}]
                 WHERE expires < %i";
            $result = $db->query($sql, Time::get());
            $stmt = $this->impl->deleteTaskStatement();
            while ($task = $result->fetchArray()) {
                if ($this->log() !== null)
                    $this->logMessage(
                        'Deleting old task',
                        $this->encodeTask($task)
                    );
                $stmt->execute([$task['RecordID']]);
            }
        } catch (Throwable $e) {
            $db->rollback();
            throw new
                Error([
                    'details' =>
                        "Queue '{$this->impl->queue}': Failed deleting expired tasks",
                    'inner' => $e
                ]);
        }
        $db->commit();

        if ($this->log() !== null)
            $this->logMessage('Done deleting expired tasks');
    }

    /**
     * Sets the value of column "status" to STATUS_FAILED for jobs that have
     * expired or for which the maximum number of attempts has been exhausted
     */
    private function markTasksFailed(): void
    {
        if ($this->log() !== null)
            $this->logMessage('Marking tasks failed');

        $db = $this->db();
        $db->beginTransaction();
        try {
            $sql =
                "SELECT *
                 FROM [{$this->impl->queue}]
                 WHERE status = %i AND
                       ( attempts >= maxAttempts OR
                         expires < %i )";
            $result = $db->query($sql, Task::STATUS_PENDING, Time::get());
            $stmt = $this->impl->markFailedStatement();
            while ($row = $result->fetchArray()) {
                $task = new Task($this->impl, $row);
                $task->update(Task::STATUS_FAILURE);
            }
        } catch (Throwable $e) {
            $db->rollback();
            throw new
                Error([
                    'details' =>
                        "Queue '{$this->impl->queue}': Failed marking tasks failed",
                    'inner' => $e
                ]);
        }
        $db->commit();

        if ($this->log() !== null)
            $this->logMessage('Done marking tasks failed');
    }

    /**
     * Writes a message to the log, if any
     *
     * @param string $message The message prefix
     * @param mixed $data An object or associative array to be JSON-encoded
     *   and appended to $message
     */
    private function logMessage($message, $params = null)
    {
        if ($this->log() !== null) {
            if ($params !== null) {
                $pruned = [];
                foreach ($params as $n => $v)
                    if ($v !== null)
                        $pruned[$n] = $v;
                $message .= ' ' . json_encode($pruned);
            }
            $this->log()->logMessage("Queue '{$this->impl->queue}': $message");
        }
    }

    /**
     * Encodesn the give database record representing a task as an associative
     * array
     *
     * @param array $row An associative array reprenting a record in the queue
     * @return stdClass
     */
    private function encodeTask(array $row): object
    {
        return (new Task($this->impl, $row))->encode();
    }

    /**
     * Returns the database connection
     *
     * @return CodeRage\Db
     */
    private function db(): \CodeRage\Db
    {
        return $this->impl->tool->db();
    }

    /**
     * Returns the log, if any
     *
     * @return CodeRage\Log
     */
    private function log(): ?Log
    {
        return $this->impl->log;
    }

    /**
     * @var CodeRage\Queue\ManagerImpl
     */
    private $impl;
}
