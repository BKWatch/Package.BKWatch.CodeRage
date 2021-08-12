<?php

/**
 * Defines the class CodeRage\Queue\BatchProcessor
 *
 * File:        CodeRage/Queue/BatchProcessor.php
 * Date:        Fri Jan  3 15:52:34 UTC 2020
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2019 CounselNow
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Queue;

use Exception;
use Throwable;
use CodeRage\Error;
use CodeRage\Log;
use CodeRage\Queue\Task;
use CodeRage\Util\Args;
use CodeRage\Util\Json;

/**
 * Base class for queue processing tools that follow a uniform pattern
 */
abstract class BatchProcessor extends \CodeRage\Tool\Tool {

    /**
     * Default batch size; the trailing undescore prevents nameing clashes
     *
     * @var int
     */
    private const DEFAULT_BATCH_SIZE_ = 1;

    /**
     * The number of seconds to sleep after starting a worker; reduces the
     * chance of database failures
     *
     * @var int
     */
    private const CREATE_WORKER_SLEEP = 1;

    /**
     * The number of seconds to sleep after checking the status of workers
     *
     * @var int
     */
    private const CHECK_WORKER_SLEEP = 1;

    /**
     * @var array
     */
    private const OPTIONS =
        [ 'taskMode', 'taskMaxAttempts', 'taskLifetime', 'taskWorkers',
          'taskBatchSize', 'taskSleep', 'taskShuffle', 'taskSessionid',
          'taskData1', 'taskData2', 'taskData3', 'taskDebug' ];

    /**
     * @var array
     */
    private const MODE_OPTIONS =
        [
            'create' =>
                [
                    'taskMaxAttempts' => 1,
                    'taskLifetime' => 1,
                    'taskShuffle' => 1
                ],
            'run' =>
                [
                    'taskMaxAttempts' => 1,
                    'taskLifetime' => 1,
                    'taskShuffle' => 1,
                    'taskBatchSize' => 1,
                    'taskSleep' => 1,
                    'taskData1' => 1,
                    'taskData2' => 1,
                    'taskData3' => 1
                ],
            'multi' =>
                [
                    'taskWorkers' => 1,
                    'taskLifetime' => 1,
                    'taskLifetime' => 1,
                    'taskShuffle' => 1,
                    'taskBatchSize' => 1,
                    'taskSleep' => 1,
                    'taskData1' => 1,
                    'taskData2' => 1,
                    'taskData3' => 1
                ],
            'worker' =>
                [
                    'taskMaxAttempts' => 1,  // Eliminate if possible
                    'taskLifetime' => 1,     // Eliminate if possible
                    'taskSessionid' => 1
                ],
            'status' => [],
            'clear' => [],
            'terminate' => []
        ];

    /**
     * @var string
     */
    private const MATCH_MODE = '/^(create|run|multi|worker|status|clear|terminate)$/';

    /**
     * Constructs a Util\TaskProcessor
     *
     * @param array $options The options array; supports all options supported
     *   by CodeRage\Tool\Tool
     */
    public function __construct($options)
    {
        parent::__construct($options);
    }

    /**
     * @param array $options The options array; supports the following options:
     *     taskMode - One of "create", "run", "multi", "worker", "status",
     *       "clear", "terminate"; defaults to "multi" if "taskWorkers" is
     *       specified, and to "run", otherwise (optional)
     *     taskMaxAttempts - The maximum number of times to attempt each task
     *       (optional)
     *     taskLifetime - The task lifetime, in seconds (optional)
     *     taskWorkers - The number of concurrent processes to run; only
     *       compatible with mode "multi" (optional)
     *     taskBatchSize - The number of tasks to assign to a worker, for task
     *       processors that do not customize task assignment; only compatible
     *       with mode "multi" (optional)
     *     taskSleep - The number of milliseconds to sleep between tasks;
     *       requires the cooperation of the derived class, if the derived class
     *       implements doProcessBatch() without using processTask(); defaults
     *       to 0
     *     taskShuffle - true to process tasks in shuffled order; requires
     *       the cooperation of the derived class; defaults to false
     *     taskSessionid - The session ID of an existing queue processing
     *       session; only compatible with mode "worker" (optional)
     *  @return mixed
     */
    public final function doExecute(array $options)
    {
        $this->queue = $this->doQueue($options);
        $this->processOptions($options);
        $this->parameters = $this->doParameters($this->pruneOptions($options));
        $mode = $options['taskMode'];
        $result = null;
        switch ($mode) {
        case 'create':
            $result = $this->create($options);
            break;
        case 'run':
            $result = $this->run($options);
            break;
        case 'multi':
            $result = $this->multi($options);
            break;
        case 'worker':
            $result = $this->worker($options);
            break;
        case 'status':
            $result = $this->status($options);
            break;
        case 'clear':
            $result = $this->clear($options);
            break;
        case 'terminate':
            $result = $this->terminate($options);
            break;
        default:
            // Can't occur
            break;
        }
        return $result;
    }

    /**
     * Executes in "create" mode
     *
     * @param array $options The options array passed to doExecute
     * @return int The number of tasks created
     */
    public final function create(array $options)
    {
        $manager =
            new Manager([
                    'queue' => $this->queue(),
                    'parameters' => $this->parameters(),
                    'maxAttempts' => $options['taskMaxAttempts'],
                    'lifetime' => $options['taskLifetime'],
                    'tool' => $this
                ]);
        return $this->createTasks($options, $manager);
    }

    /**
     * Executes in "run" mode
     *
     * @param array $options The options array passed to doExecute
     * @return mixed
     */
    public final function run(array $options)
    {
        $manager =
            new Manager([
                    'queue' => $this->queue(),
                    'parameters' => $this->parameters(),
                    'maxAttempts' => $options['taskMaxAttempts'],
                    'lifetime' => $options['taskLifetime'],
                    'tool' => $this
                ]);
        if ($this->countTasks($options) == 0) {
            $this->createTasks($options, $manager);
        }
        $retval = null;
        while (true) {
            $manager = $this->assignTasks($options);
            if ($manager === null)
                break;
            $batch = $manager->loadTasks(['owned' => true]);
            $rv = $this->processBatch($options, $manager, $batch);
            $retval = $this->aggregateResults($options, $retval, $rv);
        }
        return $retval;
    }

    /**
     * Executes in "multi" mode
     *
     * @param array $options The options array passed to doExecute()
     * @return mixed
     */
    public final function multi(array $options)
    {
        // Create tasks
        $manager =
            new Manager([
                    'queue' => $this->queue(),
                    'parameters' => $this->parameters(),
                    'maxAttempts' => $options['taskMaxAttempts'],
                    'lifetime' => $options['taskLifetime'],
                    'tool' => $this
                ]);
        if ($this->countTasks($options) == 0) {
            $this->createTasks($options, $manager);
        }

        // Encode options as string
        $workerOpts = $this->doEncodeOptions($options);
        foreach (self::OPTIONS as $opt) {
            if ( isset($options[$opt]) &&
                 array_key_exists($opt, self::MODE_OPTIONS['worker']) )
            {
                $workerOpts[$opt] = $options[$opt];
            }
        }
        $workerOpts['taskMode'] = 'worker';
        $workerOpts['taskClass'] =  str_replace('\\', '.', static::class);
        unset($workerOpts['taskWorkers']);

        // Run workers
        $workers = [];
        $hasResult = $hasError = false;
        $result = null;
        do {

            // Check existing workers
            for ($i = count($workers) - 1; $i >= 0; --$i) {
                $worker = $workers[$i];
                try {
                    if ($worker->terminated()) {
                        array_splice($workers, $i, 1);
                        $res = $worker->result();
                        $result = $this->aggregateResults($options, $result, $res);
                        $hasResult = true;
                        if ($stream = $this->log()->getStream(Log::VERBOSE)) {
                            $this->logMessage(
                                'Worker ' . $worker->sessionid() . ' ' .
                                'completed with  output ' . $json->encode($res)
                            );
                        } elseif ($stream = $this->log()->getStream(Log::INFO)) {
                            $this->logMessage(
                                'Worker ' . $worker->sessionid() . ' completed'
                            );
                        }
                    } elseif ($worker->timedOut()) {
                        array_splice($workers, $i, 1);
                        $this->logMessage(
                            'Worker ' . $worker->sessionid() . ' timed out'
                        );
                    }
                } catch (Throwable $e) {
                    $this->logError(
                        new Error([
                                'details' =>
                                    'Failed processing worker ' .
                                    $worker->sessionid(),
                                'inner' => $e
                            ])
                    );
                    $hasError = true;
                }
            }

            // Create new workers
            $manager->clearSessions();
            while (count($workers) < $options['taskWorkers']) {
                $mngr = $this->assignTasks($options);
                if ($mngr !== null) {
                   $worker = new Worker($mngr, $workerOpts);
                   $worker->start();
                   $workers[] = $worker;
                } else {
                    break;
                }
                if (count($workers) < $options['taskWorkers']) {
                    self::sleep(self::CREATE_WORKER_SLEEP);
                }
            }
            if (!empty($workers)) {
                self::sleep(self::CHECK_WORKER_SLEEP);
            }

        } while (!empty($workers));

        if ($hasError && !$hasResult) {
            throw new
                Error([
                    'status' => 'INTERNAL_ERROR',
                    'message' => 'All workers failed'
                ]);
        }

        return $result;
    }

    /**
     * Executes in "worker" mode
     *
     * @param array $options The options array passed to doExecute()
     * @return mixed
     */
    public final function worker(array $options)
    {
        $manager =
            new Manager([
                    'queue' => $this->queue(),
                    'parameters' => $this->parameters(),
                    'maxAttempts' => $options['taskMaxAttempts'],
                    'lifetime' => $options['taskLifetime'],
                    'sessionid' => $options['taskSessionid'],
                    'tool' => $this
                ]);
        $batch = $manager->loadTasks(['owned' => true]);
        return $this->processBatch($options, $manager, $batch);
    }

    /**
     * Executes in "status" mode implemented by calling doStatus()
     *
     * @param array $options The options array passed to doExecute()
     * @return An associative array reference with keys "success", "failure",
     *   and "pending", indicating the number of jobs with each status
     */
    public final function status(array $options)
    {
        $queue = $this->queue();
        $result =
            $this->db()->query(
                "SELECT status, COUNT(*)
                 FROM [$queue]
                 WHERE parameters = %s
                 GROUP BY status",
                $this->parameters()
            );
        $totals =
            [
                'success' => 0,
                'failure' => 0,
                'pending' => 0
            ];
        $labels =
            [
                Task::STATUS_SUCCESS => 'success',
                Task::STATUS_FAILURE => 'failure',
                Task::STATUS_PENDING => 'pending'
            ];
        while ($row = $result->fetchRow()) {
            $totals[$labels[$row[0]]] = $row[1];
        }
        return $this->doStatus($options, $totals);
    }

    /**
     * Executes in "clear" mode
     *
     * @param array $options The options array passed to doExecute()
     * @return The number of deleted sessions (subject to race conditions)
     */
    public final function clear(array $options)
    {
        $queue = $this->queue();
        $params = $this->parameters();
        $count =
            $this->db()->fetchValue(
                "SELECT COUNT(*)
                 FROM [$queue]
                 WHERE sessionid IS NOT NULL AND
                       parameters = %s AND
                       status = %i",
                $params, Task::STATUS_PENDING
            );
        $this->db()->query(
            "UPDATE [$queue]
             SET sessionid = NULL,
                 attempts = attempts + 1
             WHERE sessionid IS NOT NULL AND
                   parameters = %s AND
                   status = %i",
            $params,
            Task::STATUS_PENDING
        );
        $this->db()->query(
            "UPDATE [$queue]
             SET sessionid = NULL
             WHERE parameters = %s AND
                   status != %i",
            $params,
            Task::STATUS_PENDING
        );
        return ['clearedSessions' => $count];
    }

    /**
     * Executes in "terminate" mode
     *
     * @param array $options The options array passed to doExecute()
     * @return An associative array reference with key "count" indicating the
     *   number of deleted jobs (subject to race conditions)
     */
    public final function terminate(array $options)
    {
        $queue = $this->queue();
        $params = $this->parameters();
        $count =
            $this->db()->fetchValue(
                "SELECT COUNT(*)
                 FROM [$queue]
                 WHERE parameters = %s",
                $params
            );
        $this->db()->delete($queue, ['parameters' => $params]);
        return ['deletedTasks' => $count];
    }

    /**
     * Returns the queue name
     */
    public final function queue()
    {
        return $this->queue;
    }

    /**
     * Returns the value stored in the "parameters" column of the queue
     */
    public final function parameters()
    {
        return $this->parameters;
    }

    /**
     * Creates tasks in the queue; implemented by calling doCreateTasks()
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @param CodeRage\Queue\Manager $manager An instance of
     *   CodeRage\Queue\Manager
     */
    public final function createTasks(array $options, Manager $manager)
    {
        return $this->doCreateTasks($options, $manager);
    }

    /**
     * Returns an instance of CodeRage\Queue\Manager that owns a collection of
     * tasks to be processed by a worker
     *
     * @param array $options The options array passed to doExecute, after
     *   processing
     * @return CodeRage\Queue\Manager
     */
    public final function assignTasks(array $options)
    {
        return $this->doAssignTasks($options);
    }

    /**
     * Processes the given task; implemented by calling doProcessTask()
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @param CodeRage\Queue\Manager $manager An instance of
     *   CodeRage\Queue\Manager
     * @param CodeRage\Queue\Task $task The task
     * @return mixed The result of processing $task
     */
    public final function processTask(array $options, Manager $manager, Task $task)
    {
        $opts = $this->pruneOptions($options);
        $result = $error = null;
        $status = Task::STATUS_SUCCESS;
        try {
            $result = $this->doProcessTask($opts, $manager, $task);
            if ($options['taskSleep'] > 0) {
                $this->sleep($options['taskSleep']);
            }
        } catch (Throwable $e) {
            $error = Error::wrap($e);
            $status = Task::STATUS_PENDING;
            $this->logError($error);
        }
        $task->update($status, ['error' => $error]);
        return $result;
    }

    /**
     * Processes the given list of tasks; implemented by calling
     * doProcessBatch()
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @param CodeRage\Queue\Manager $manager An instance of
     *   CodeRage\Queue\Manager
     * @param array $batch A list of instances of CodeRage\Queue\Task
     * @return mixed The result of processing $batch
     */
    public final function processBatch(array $options, Manager $manager, array $batch)
    {
        return $this->doProcessBatch($options, $manager, $batch);
    }

    /**
     * Combines the two given queue processing results into a single result;
     * returns an null by default
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @param mixed $a The result of processing a task or batch or task, or an
     *   exception thrown by doProcessTask; may be undefined if $b is the result
     *   of processing the first task in a batch
     * @param mixed $b The result of processing a task or batch or tasks, or
     *   an exception thrown by doProcessTask()
     * @return mixed The result of combining $a and $b
     */
    public final function aggregateResults(array $options, $a, $b)
    {
        return $this->doAggregateResults($options, $a, $b);
    }

    /**
     * Processes and validates options to doExecute(), excluding the options
     * common to all task processors. A no-op by default.
     *
     * @param array $options A copy of the options array passed to doExecute,
     *   excluding common options
     */
    protected function doProcessOptions(array &$options)
    {
        // No-op
    }

    /**
     * Encodes the given options as strings so that they can be passed to child
     * processes on the command-line
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @return A string-valued associative array
     */
    protected function doEncodeOptions(array $options)
    {
        // No-op
    }

    /**
     * Returns the default value of the "taskMaxAttempts" option, if any;
     * returns an null by default
     *
     * @return int
     */
    protected function doDefaultMaxAttempts()
    {
        return null;
    }

    /**
     * Returns the default value of the "taskLifetime" option, if any; returns
     * null by default
     *
     * @return int
     */
    protected function doDefaultLifetime()
    {
        return null;
    }

    /**
     * Returns the default value of the "taskBatchSize" option, if any; returns
     * an null by default
     *
     * @return int
     */
    protected function doDefaultBatchSize()
    {
        return null;
    }

    /**
     * Returns the queue name
     */
    protected abstract function doQueue(array $options);

    /**
     * Returns the value to be stored in the "parameters" column of the
     * queue; by default, returns an empty string if the given options array is
     * empty, and otherwise calls doEncodeOptions() and JSON-encodes the result
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing, excluding the options common to all task processors
     * @return The parameters, as a string
     */
    protected function doParameters(array $options)
    {
        if (empty($options))
            return '';
        $options = $this->doEncodeOptions($options);
        return Json::encode($options, ['throwOnError' => true]);
    }

    /**
     * Creates tasks in the queue
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @param CodeRage\Queue\Manager $manager An instance of
     *   CodeRage\Queue\Manager
     * @return int The number of tasks created
     */
    protected abstract function doCreateTasks(array $options, Manager $manager): int;

    /**
     * Returns an instance of CodeRage\Queue\Manager that owns a collection of
     * tasks to be processed by a worker; returns an null if no tasks are
     * available
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @return CodeRage\Queue\Manager
     */
    protected function doAssignTasks(array $options)
    {
        $manager =
            new Manager([
                    'queue' => $this->queue(),
                    'parameters' => $this->parameters(),
                    'maxAttempts' => $options['taskMaxAttempts'],
                    'lifetime' => $options['taskLifetime'],
                    'tool' => $this
                ]);
        $tasks =
            $manager->claimTasks([
                'maxTasks' => $options['taskBatchSize'],
                'data1' => $options['taskData1'] ?? null,
                'data2' => $options['taskData2'] ?? null,
                'data3' => $options['taskData3'] ?? null
            ]);
        return $tasks > 0 ? $manager : null;
    }

    /**
     * Processes the given tasks; a no-op by default. This method is not
     * responsible for updating the status of $task.
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @param CodeRage\Queue\Manager $manager An instance of
     *   CodeRage\Queue\Manager
     * @param CodeRage\Queue\Manager $task The task
     * @return mixed The result of processing $task
     * @throws CodeRage\Error if processing fails
     */
    protected function doProcessTask(array $options, Manager $manager, Task $task)
    {
        // No-op
    }

    /**
     * Processes the given list of tasks; by default, processes each task by
     * calling doProcessTask and aggregates the results using aggregateResults.
     * This method is responsible for updating the status of the tasks in
     * $batch.
     *
     * @param array $options The options array passed to doExecute()`, after
     *   processing
     * @param CodeRage\Queue\Manager $manager An instance of
     *   CodeRage\Queue\Manager
     * @param array $batch A list of instances of CodeRage\Queue\Task
     * @return mixed The result of processing $batch
     */
    protected function doProcessBatch(array $options, Manager $manager, array $batch)
    {
        $result = null;
        foreach ($batch as $task) {
            $res = $this->processTask($options, $manager, $task);
            $result = $this->aggregateResults($options, $result, $res);
        }
        return $result;
    }

    /**
     * Combines the two given queue processing results into a single result;
     * returns null by default
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @param mixed $a The result of processing a task or batch or task, or an
     *   exception thrown by doProcessTask; may be undefined if $b is the result
     *   of processing the first task in a batch
     * @param mixed $b The result of processing a task or batch or tasks, or
     *   an exception thrown by doProcessTask(
     * @return mixed The result of combining $a and $b
     */
    protected function doAggregateResults(array $options, $a, $b)
    {
        return null;
    }

    /**
     * Returns aggregate information about the tasks in the queue with
     * parameters equal to this instance's parameters; by default returns
     * $totals
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     * @param array $totals An associative array reference with keys "success",
     *   "failure", and "pending", indicating the number of tasks with each
     *   status
     * @return array An associative array reference
     */
    protected function doStatus(array $options, $totals)
    {
        return $totals;
    }

    /**
     * Sleeps for the specified number of milliseconds
     *
     * @param int $ms
     */
    protected static function sleep($ms)
    {
        usleep($ms * 1000);
    }

    /**
     * Validates and processes options for doExecute()
     *
     * @param array $options The options array passed to doExecute
     */
    private function processOptions(array &$options)
    {
        // Check consistency before applying default values
        $mode = Args::checkKey($options, 'taskMode', 'string');
        if ($mode !== null && !preg_match(self::MATCH_MODE, $mode)) {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Invalid taskMode: $mode"
                ]);
        }
        foreach (self::OPTIONS as $opt) {
            if ( isset($options[$opt]) &&
                 $opt !== 'taskMode' &&
                 !array_key_exists($opt, self::MODE_OPTIONS[$mode]) )
            {
                throw new
                    Error([
                        'status' => 'INCONSISTENT_PARAMETERS',
                        'details' =>
                            "The option '$opt' is incompatible with mode " .
                            "'$mode'"
                    ]);
            }
        }

        // Validate options
        $maxAttempts =
            Args::checkIntKey($options, 'taskMaxAttempts', [
                'default' => $this->doDefaultMaxAttempts()
            ]);
        if ($maxAttempts !== null && $maxAttempts <= 0) {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid taskMaxAttempts: expected positive integer; " .
                        "found $maxAttempts"
                ]);
        }
        $lifetime =
            Args::checkIntKey($options, 'taskLifetime', [
                'default' => $this->doDefaultLifetime()
            ]);
        if ($lifetime !== null && $lifetime <= 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid taskLifetime: expected positive integer; " .
                        "found $lifetime"
                ]);
        $workers = Args::checkIntKey($options, 'taskWorkers');

        if ($workers !== null && $workers                 <               1) {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid taskWorkers: expected integer greater than " .
                        "1; found $workers"
                ]);
        }
        Args::checkIntKey($options, 'taskBatchSize', [
            'default' => $this->doDefaultBatchSize() ?? self::DEFAULT_BATCH_SIZE_
        ]);
        $sleep =
            Args::checkIntKey($options, 'taskSleep', [
                'default' => 0
            ]);
        if ($sleep !== null && $sleep < 0) {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid taskWorkers: expected non-negative integer; " .
                        "found $sleep"
                ]);
        }
        Args::checkBooleanKey($options, 'taskShuffle', [
            'default' => false
        ]);
        $sessionId = Args::checkKey($options, 'taskSessionid', 'string');
        Args::checkKey($options, 'taskData1', 'string|list[string]');
        Args::checkKey($options, 'taskData2', 'string|list[string]');
        Args::checkKey($options, 'taskData3', 'string|list[string]');
        Args::checkKey($options, 'taskDebug', 'string');

        // Calculate mode
        if ($mode === null) {
            $options['taskMode'] = $workers !== null ? 'multi' : 'run';
        }

        // Check relationships between options
        if ($maxAttempts === null && $lifetime === null) {
            throw new
                Error([
                    'status' => 'MISSING_PARAMETER',
                    'details' => "Missing 'taskMaxAttempts' or 'taskLifetime'"
                ]);
        }
        if ($workers !== null && $mode != 'multi') {
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        "The option 'taskWorkers' is incompatible with mode " .
                        "'$mode'"
                ]);
        }
        if (($sessionId !== null) != ($mode == 'worker')) {
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' => $sessionId !== null ?
                        "The option 'taskSessionid' is incompatible with mode " .
                            "'$mode'" :
                        "Missing session ID"
                ]);
        }

        // Process processor-specific options
        $procOpts = $this->pruneOptions($options);
        foreach ($procOpts as $opt => $v) {
            unset($options[$opt]);
        }
        $this->doProcessOptions($procOpts);
        $options += $procOpts;
    }

    /**
     * Returns a copy of the given options array, with the queue processing
     * options removed
     *
     * @param array $options The options array passed to doExecute(), after
     *   processing
     */
    private function pruneOptions(array $options)
    {
        foreach (self::OPTIONS as $opt)
            unset($options[$opt]);
        return $options;
    }

    /**
     * Returns the number of tasks in the queue with parameters equal to the
     * current parameters
     *
     * @param array $options The options array passed to doExecute()
     * @return int
     */
    private function countTasks(array $options): int
    {
        $queue = $this->queue();
        return
            $this->db()->fetchValue(
                "SELECT COUNT(*) {i} FROM [$queue] WHERE parameters = %s",
                $this->parameters()
            );
    }

    /**
     * @var string
     */
    private $queue;
}
