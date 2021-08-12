<?php

/**
 * Defines the class CodeRage\Db.
 *
 * File:        CodeRage/Db.php
 * Date:        Wed May 02 18:51:52 MDT 2007
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2015 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage;

use PDO;
use PDOException;
use Throwable;
use CodeRage\Config;
use CodeRage\Db\Hook;
use CodeRage\Db\Params;
use CodeRage\File;
use CodeRage\Util\Args;
use CodeRage\Util\Array_;

/**
 * @ignore
 */

/**
 * Represents a database connection
 */
final class Db extends \CodeRage\Db\Object_ {

    /**
     * Maps MDB2 DBMS names to PDO DBMS names
     *
     * @var array
     */
    private const DBMS_MAPPING =
        [
            'mysql' => 'mysql',
            'mysqli' => 'mysql',
            'mssql' => 'mssql',
            'odbc' => 'odbc',
            'sqlsrv' => 'sqlsrv',
            'ibase' => 'firebird',
            'oci8' => 'oci',
            'pgsql' => 'pgsql',
            'sqlite' => 'sqlite'
        ];

    /**
     * @var array
     */
    private const OPTIONS =
        [ 'params' => 1, 'dbms' => 1, 'host' => 1, 'port' => 1, 'username' => 1,
          'password' => 1, 'database' => 1, 'useCache' => 1 ];

    /**
     * Constructs a CodeRage\Db
     *
     * @param array $options The options array; supports the following options:
     *     params - An instance of CodeRage\Db\Params
     *     dmbs - The database engine, e.g., 'mysql', 'mssql, 'pgsql'
     *     host - The host name or IP addresss of the server
     *     port - The port
     *     username - The username
     *     password - The password
     *     database - the initial database
     *     useCache - true to use a cached connection if available; defaults to
     *       true if no other options are supplied, and otherwise must be absent
     *       (optional)
     *   At most one of "params" and "dmbs" may be supplied. If "dbms"
     *   is supplied, the options "host", "username", "password", and "database"
     *   must also be supplied. If neither "params" nor "dmbs" is suppled,
     *   the values of the parameters "dbms" through "database" are fetched from
     *   the project configuration, with configuration variable name formed
     *   by prefixing the connection parameter name with "db."
     */
    public function __construct(array $options = [])
    {
        foreach ($options as $n => $ignore) {
            if (!array_key_exists($n, self::OPTIONS)) {
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' => "Unsupported option: $n"
                    ]);
            }
        }

        // Process "useCache" first and remove it from the options array,
        // to optimize the common case where $options is empty
        $useCache =
            Args::checkKey($options, 'useCache', 'boolean', [
                'label' => 'use cache flag',
                'default' => null,
                'unset' => true
            ]);
        $params = $cacheId = null;
        if (!empty($options)) {
            if ($useCache !== null) {
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' =>
                            "The option 'useCache' is not supported when " .
                            "connection parameters are specified explicitly"
                    ]);
            }
            $opt = Args::uniqueKey($options, ['params', 'dbms']);
            $params = $opt == 'params' ?
                Args::checkKey($options, 'params', 'CodeRage\Db\Params') :
                new Params($options);
        } else {
            $config = Config::current();
            $cacheId = self::cacheId($config);
            if (!isset(self::$paramsCache[$cacheId])) {
                self::$paramsCache[$cacheId] = Params::create($config);
            }
            $params = self::$paramsCache[$cacheId];
            $useCache = $useCache ?? true;
        }
        if (!array_key_exists($params->dbms(), self::DBMS_MAPPING)) {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'Unsupported DBMS: ' . $params->dbms()
                ]);
        }
        $this->params = $params;
        $this->useCache = $useCache;
        $this->nestable = true;
        $this->queryProcessor = new Db\QueryProcessor($this);
        $this->cacheId = $cacheId;
    }

    public function __destruct()
    {
        self::$hooks = null;
    }

    /**
     * Returns the connection parameters
     *
     * @return CodeRage\Db\Params
     */
    public function params() { return $this->params; }

    /**
     * Begins a transaction
     *
     * @throws CodeRage\Error
     */
    public function beginTransaction()
    {
        $transactionDepth = self::transactionDepth();
        $conn = $this->connection();
        if (!isset($transactionDepth[$conn]))
            $transactionDepth[$conn] = 0;
        if (!$this->nestable && $transactionDepth[$conn] > 0)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'details' =>
                        'This instance of CodeRage\Db does not support ' .
                        'nested transactions'
                ]);
        if ($transactionDepth[$conn] == 0) {
            try {
                $conn->beginTransaction();
            } catch (PDOException $e) {
                throw new
                    Error([
                        'status' => 'DATABASE_ERROR',
                        'details' => 'Failed beginning transaction',
                        'inner' => $e
                    ]);
            }
        }
        $transactionDepth[$conn] += 1;
    }

    /**
     * Commits a transaction
     *
     * @throws CodeRage\Error
     */
    public function commit()
    {
        $transactionDepth = self::transactionDepth();
        $conn = $this->connection();
        if (!isset($transactionDepth[$conn]))
            $transactionDepth[$conn] = 0;
        $transactionDepth[$conn] -= 1;
        if ($transactionDepth[$conn] == 0) {
            try {
                $conn->commit();
            } catch (PDOException $e) {
                throw new
                    Error([
                        'status' => 'DATABASE_ERROR',
                        'details' => 'Failed committing transaction',
                        'inner' => $e
                    ]);
            }
        }
    }

    /**
     * Rolls back a transaction
     *
     * @throws CodeRage\Error
     */
    public function rollback()
    {
        $transactionDepth = self::transactionDepth();
        $conn = $this->connection();
        if (!isset($transactionDepth[$conn]))
            $transactionDepth[$conn] = 0;
        $transactionDepth[$conn] -= 1;
        if ($transactionDepth[$conn] == 0) {
            try {
                $conn->rollback();
            } catch (PDOException $e) {
                throw new
                    Error([
                        'status' => 'DATABASE_ERROR',
                        'details' => 'Failed rollinbg back transaction',
                        'inner' => $e
                    ]);
            }
        }
    }

    /**
     * Prepares the specified SQL query, returning a statement object
     *
     * @param string $sql A SQL query with embedded placeholders. Identifiers
     *   can be quoted using  square brackets; placeholders have the form
     *   '%c', where c is one of 'i', 'f', 'd', 's', or 'b', with the following
     *   interpretation:
     *      i - integer
     *      f - float
     *      d - decimal
     *      s - string
     *      b - blob
     *   The expected data types of columns in the result set can be indicated
     *   by inserting an expression of the form '{c}' after the column
     *   expression, where 'c' has the same interpretation as above.
     * @return CodeRage\Db\Statement
     * @throws CodeRage\Error
     */
    public function prepare($sql)
    {
        list($query, $columns, $params) = $this->queryProcessor->process($sql);
        try {
            $statement = $this->connection()->prepare($query);
        } catch (PDOException $e) {
            throw new
                Error([
                    'status' => 'DATABASE_ERROR',
                    'details' => 'Failed preparing statement',
                    'inner' => $e
                ]);
        }
        return new Db\Statement($statement, $params, $columns);
    }

    /**
     * Executes the specified SQL query
     *
     * @param string $sql A SQL query with embedded placeholders. Identifiers
     *   can be quoted using  square brackets; placeholders have the form
     *   '%c', where c is one of 'i', 'f', 'd', 's', or 'b', with the following
     *   interpretation:
     *      i - integer
     *      f - float
     *      d - decimal
     *      s - string
     *      b - blob
     *   The expected data types of columns in the result set can be indicated
     *   by inserting an expression of the form '{c}' after the column
     *   expression, where 'c' has the same interpretation as above.
     * @param array $args The values to bind to the query's embedded
     *   placeholders; these values may also be passed as individual function
     *   arguments
     * @return CodeRage\Db\Result
     * @throws CodeRage\Error
     */
    public function query($sql, $args = null)
    {
        if (!is_array($args)) {
            $args = func_get_args();
            array_shift($args);
        }
        list($query, $columns) = $this->queryProcessor->process($sql, $args);
        $conn = $this->connection();
        if (self::$hooks !== null && isset(self::$hooks[$conn]))
            foreach (self::$hooks[$conn] as $h)
                $h->preQuery($query);
        try {
            $result = $this->connection()->query($query);
        } catch (PDOException $e) {
            throw new
                Error([
                    'status' => 'DATABASE_ERROR',
                    'details' => 'Failed executing query',
                    'inner' => $e
                ]);
        }
        if (self::$hooks !== null && isset(self::$hooks[$conn]))
            foreach (self::$hooks[$conn] as $h)
                $h->postQuery($query);
        return new Db\Result($result, $columns);
    }

    /**
     * Inserts a record with the given collection of values into the named
     * table, automatically populating the 'CreationDate' column. Returns
     * the value of the 'RecordII' column.
     *
     * @param string $table The table name
     * @param array $values An associative array of values, indexed by column
     *   name
     * @throws CodeRage\Error if an error occurs
     */
    public function insert($table, $values)
    {
        $cols = [];  // Column names
        $vals = [];  // Column values
        $phs = [];   // Placeholders
        $now = null;
        if (!array_key_exists('CreationDate', $values)) {
            if (!$now)
                $now = \CodeRage\Util\Time::get();
            $cols[] = 'CreationDate';
            $vals[] = $now;
            $phs[] = '%i';
        }
        foreach ($values as $c => $v) {
            $cols[] = $c;
            $vals[] = $v;
            $phs[] = self::placeholder($v);
        }
        $sql =
            "INSERT INTO [$table] ([" . join('],[', $cols) . ']) ' .
            'VALUES (' . join(',', $phs) . ');';
        $this->query($sql, $vals);
        return $this->lastInsertId();
    }

    /**
     * Updates all records satisfying the given condition so that they match the
     * given collection of values.
     *
     * @param string $table The table name
     * @param array $values An associative array of values, indexed by column
     *   name
     * @param mixed $where The value of the 'RecordID' column, or an associative
     *   array mapping column names to values
     * @throws CodeRage\Error if no record matching the given conditions exists,
     *   or if an error occurs.
     */
    public function update($table, $values, $where)
    {
        if (is_int($where))
            $where = ['RecordID' => $where];

        // Check whether records exist
        $conds = [];  // Conditions
        $cvals = [];
        foreach ($where as $c => $v) {
            if (!preg_match('/^[_a-zA-Z0-9]+$/', $c))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' => "Invalid column name: $c"
                    ]);
            $conds[] = "[$c] = " . self::placeholder($v);
            $cvals[] = $v;
        }
        $sql = "SELECT COUNT(*)
                FROM [$table]
                WHERE " . join(' AND ', $conds);
        if ($this->fetchValue($sql, $cvals) == 0) {
            throw new
                Error([
                    'status' => 'OBJECT_DOES_NOT_EXIST',
                    'details' => "Failed updating $table: no such record"
                ]);
        }

        // Update record
        $set = [];
        $svals = [];
        foreach ($values as $c => $v) {
            if (!preg_match('/^[_a-zA-Z0-9]+$/', $c))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' => "Invalid column name: $c"
                    ]);
            $set[] = "[$c] = " . self::placeholder($v);
            $svals[] = $v;
        }
        $sql =
            "UPDATE [$table]
             SET " . join(', ', $set) . "
             WHERE " . join(' AND ', $conds);
        $this->query($sql, array_merge($svals, $cvals));
    }

    /**
     * If there exists a record in the named table matching the given
     * conditions, updates it to match the given collection of values;
     * otherwise, inserts a record having the combined collection of values.
     * Automatically populates the CreationDate column, as appropriate
     *
     * @param string $table The table name
     * @param array $values An associative array of values, indexed by column
     *   name
     * @param mixed $where The value of the 'RecordID' column, or an associative
     *   array mapping column names to values
     * @return The value of the RecordID column of the new record, if an new
     *   record was created, the RecordID column of the updated record, if a
     *   single record was updated, or an array containing the values of the
     *   RecordID columns of each record that was updated, if there was more
     *   than one.
     * @throws CodeRage\Error if an error occurs.
     */
    public function insertOrUpdate($table, $values, $where)
    {
        if (is_int($where))
            $where = ['RecordID' => $where];

        // Check whether records exist
        $conds = [];  // Conditions
        $whereVals = [];
        foreach ($where as $c => $v) {
            if (!preg_match('/^[_a-zA-Z0-9]+$/', $c))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' => "Invalid column name: $c"
                    ]);
            $conds[] = "[$c] = " . self::placeholder($v);
            $whereVals[] = $v;
        }
        $sql = "SELECT RecordID
                FROM [$table]
                WHERE " . join(' AND ', $conds);
        $rows = $this->fetchAll($sql, $whereVals);

        // Update or insert
        if (sizeof($rows)) {
            if (sizeof($values)) {
                $set = [];
                $setVals = [];
                foreach ($values as $c => $v) {
                    if (!preg_match('/^[_a-zA-Z0-9]+$/', $c))
                        throw new
                            Error([
                                'status' => 'INVALID_PARAMETER',
                                'details' => "Invalid column name: $c"
                            ]);
                    $set[] = "[$c] = " . self::placeholder($v);
                    $setVals[] = $v;
                }
                $sql =
                    "UPDATE [$table]
                     SET " . join(', ', $set) . "
                     WHERE " . join(' AND ', $conds);
                $vals = array_merge($setVals, $whereVals);
                $this->query($sql, $vals);
            }
            return sizeof($rows) > 1 ? array_merge($rows) : $rows[0][0];
        } else {
            return $this->insert($table, $values + $where);
        }
    }

    /**
     * Deletes all records from the named table satisying the specified
     * condition
     *
     * @param string $table The table name
     * @param mixed $where The value of the 'RecordID' column, or an associative
     *   array mapping column names to values
     * @param bool $nothrow True if no exception should be thrown if an error
     *   occurs
     * @return boolean true for success
     */
    public function delete($table, $where, $nothrow = false)
    {
        if (!is_array($where))
            $where = ['RecordID' => $where];
        $conds = [];  // Conditions
        $vals = [];
        foreach ($where as $c => $v) {
            if (!preg_match('/^[_a-zA-Z0-9]+$/', $c)) {
                if (!$nothrow)
                    throw new
                        Error([
                            'status' => 'INVALID_PARAMETER',
                            'details' => "Invalid column name: $c"
                        ]);
                return false;
            }
            $conds[] = "[$c] = " . self::placeholder($v);
            $vals[] = $v;
        }
        $sql = "DELETE FROM [$table] WHERE " . join(' AND ', $conds) . ";";
        try {
            $this->query($sql, $vals);
        } catch (\Throwable $e) {
            if (!$nothrow)
                throw $e;
            return false;
        }
        return true;
    }

    /**
     * Executes the given query, returning the first column of the first row
     * of results
     *
     * @param string $sql A SQL query with embedded placeholders. Identifiers
     *   can be quoted using  square brackets; placeholders have the form
     *   '%c', where c is one of 'i', 'f', 'd', 's', or 'b', with the following
     *   interpretation:
     *      i - integer
     *      f - float
     *      d - decimal
     *      s - string
     *      b - blob
     *   The expected data types of columns in the result set can be indicated
     *   by inserting an expression of the form '{c}' after the column
     *   expression, where 'c' has the same interpretation as above.
     * @param array $args The values to bind to the query's embedded
     *   placeholders; these values may also be passed as individual function
     *   arguments
     * @return mixed
     */
    public function fetchValue($sql, $args = null)
    {
        if (!is_array($args)) {
            $args = func_get_args();
            array_shift($args);
        }
        $row = $this->fetchFirstRow($sql, $args);
        return $row[0];
    }

    /**
     * Executes the given query, returning the first row of results as an
     * indexed array by default or returned associative array or an stdClass
     * object based on the value of $mode parameter
     *
     * @param string $sql A SQL query with embedded placeholders. Identifiers
     *   can be quoted using  square brackets; placeholders have the form
     *   '%c', where c is one of 'i', 'f', 'd', 's', or 'b', with the following
     *   interpretation:
     *      i - integer
     *      f - float
     *      d - decimal
     *      s - string
     *      b - blob
     *   The expected data types of columns in the result set can be indicated
     *   by inserting an expression of the form '{c}' after the column
     *   expression, where 'c' has the same interpretation as above.
     * @param array $args The values to bind to the query's embedded
     *   placeholders; these values may also be passed as individual function
     *   arguments
     * @param int $mode One of the constants CodeRage\Db::FETCHMODE_XXX,
     *   indicating how rows are represented; defaults to FETCHMODE_ORDERED
     * @return array
     */
    public function fetchFirstRow($sql, $args, $mode = self::FETCHMODE_ORDERED)
    {
        if (!is_array($args)) {
            $args = func_get_args();
            array_shift($args);
            $mode = self::FETCHMODE_ORDERED;
        }
        try {
            $result = $this->query($sql, $args);
            $row = $result->fetchRow($mode);
            $result->free();
        } catch (PDOException $e) {
            throw new
                Error([
                    'status' => 'DATABASE_ERROR',
                    'details' => 'Failed fetching first row of data',
                    'inner' => $e
                ]);
        }
        return $row;
    }

    /**
     * Executes the given query, returning the first row of results as an
     * associative array
     *
     * @param string $sql A SQL query with embedded placeholders. Identifiers
     *   can be quoted using  square brackets; placeholders have the form
     *   '%c', where c is one of 'i', 'f', 'd', 's', or 'b', with the following
     *   interpretation:
     *      i - integer
     *      f - float
     *      d - decimal
     *      s - string
     *      b - blob
     *   The expected data types of columns in the result set can be indicated
     *   by inserting an expression of the form '{c}' after the column
     *   expression, where 'c' has the same interpretation as above.
     * @param array $args The values to bind to the query's embedded
     *   placeholders; these values may also be passed as individual function
     *   arguments
     * @return mixed
     */
    public function fetchFirstArray($sql, $args = null)
    {
        if (!is_array($args)) {
            $args = func_get_args();
            array_shift($args);
        }
        return $this->fetchFirstRow($sql, $args, self::FETCHMODE_ASSOC);
    }

    /**
     * Executes the given query, returning the first row of results as an
     * object
     *
     * @param string $sql A SQL query with embedded placeholders. Identifiers
     *   can be quoted using  square brackets; placeholders have the form
     *   '%c', where c is one of 'i', 'f', 'd', 's', or 'b', with the following
     *   interpretation:
     *      i - integer
     *      f - float
     *      d - decimal
     *      s - string
     *      b - blob
     *   The expected data types of columns in the result set can be indicated
     *   by inserting an expression of the form '{c}' after the column
     *   expression, where 'c' has the same interpretation as above.
     * @param array $args The values to bind to the query's embedded
     *   placeholders; these values may also be passed as individual function
     *   arguments
     * @return mixed
     */
    public function fetchFirstObject($sql, $args = null)
    {
        if (!is_array($args)) {
            $args = func_get_args();
            array_shift($args);
        }
        return $this->fetchFirstRow($sql, $args, self::FETCHMODE_OBJECT);
    }

    /**
     * Executes the given query, returning the collection of results as
     * a two-dimensional array
     *
     * @param string $sql A SQL query with embedded placeholders. Identifiers
     *   can be quoted using  square brackets; placeholders have the form
     *   '%c', where c is one of 'i', 'f', 'd', 's', or 'b', with the following
     *   interpretation:
     *      i - integer
     *      f - float
     *      d - decimal
     *      s - string
     *      b - blob
     *   The expected data types of columns in the result set can be indicated
     *   by inserting an expression of the form '{c}' after the column
     *   expression, where 'c' has the same interpretation as above.
     * @param mixed $mode One of the constants FETCHMODE_ORDERED or
     *   FETCHMODE_ASSOC, or an options array with keys among:
     *     mode - On of the constants FETCHMODE_ORDERED or FETCHMODE_ASSOC;
     *       defaults to FETCHMODE_ORDERED
     *     column - The name or position the column whose value should be
     *       included in the return value, in place of the full row, as an
     *       string
     *   The options "mode" and "column" are incompatible.
     * @return mixed
     */
    public function fetchAll($sql, $args = [], $mode = self::FETCHMODE_ORDERED)
    {
        if (!is_array($args)) {
            $args = func_get_args();
            array_shift($args);
            $mode = self::FETCHMODE_ORDERED;
        }
        Args::check($mode, 'int|map', 'mode');
        $column = null;
        if (is_array($mode)) {
            $options = $mode;
            Args::checkKey($options, 'mode', 'int');
            Args::checkKey($options, 'column', 'int|string');
            list($mode, $column) = Array_::values($options, ['mode', 'column']);
            if ($mode === null) {
                $mode = $column === null ?
                    self::FETCHMODE_ORDERED :
                    ( is_int($column) ?
                          self::FETCHMODE_ORDERED :
                          self::FETCHMODE_ASSOC );
            } elseif ( $column !== null &&
                       is_int($column) != ($mode == self::FETCHMODE_ORDERED) )
            {
                $expected = $mode == self::FETCHMODE_ORDERED ?
                    'int' :
                    'string';
                throw new
                    Error([
                        'status' => 'INCONSISTENT_PARAMETERS',
                        'details' =>
                            "Invalid 'column' option: expected $expected; " .
                            "found $column"
                    ]);
            }
        }
        $stm = $this->query($sql, $args);
        try {
            $results = $stm->fetchAll($mode);
            foreach ($results as $i => &$result) {
                if ($column !== null)
                    $results[$i] = $result[$column] ?? null;
            }
        } catch (PDOException $e) {
            throw new
                Error([
                    'status' => 'DATABASE_ERROR',
                    'inner' => $e
                ]);
        }
        return $results;
    }

    /**
     * Executes the given calback inside a transaction
     *
     * @param callable $func A callable with a single parameter of type
     *   CodeRage\Db; if will be passed this instance as argument
     * @param array $options The options array; currently no options are
     *   supported
     * @return The return value of $func
     */
    public function runInTransaction($func, array $options = [])
    {
        Args::check($func, 'callable', 'callback');
        $this->beginTransaction();
        $result = null;
        try {
            $result = $func($this);
        } catch (Throwable $e) {
            $this->rollback();
            throw $e;
        }
        $this->commit();
        return $result;
    }

    /**
     * Returns the value most recently inserted into an auto-increment or
     * identity column.
     *
     * @return int
     */
    public function lastInsertId()
    {
        return (int) $this->connection()->lastInsertId();
    }

    /**
     * Quotes the given string for inclusion in a SQL query.
     *
     * @param string $value
     * @return string
     */
    public function quote($value)
    {
        return $this->connection()->quote($value);
    }

    /**
     * Quotes the given string for inclusion in a SQL query. Implementation
     * adapted from PEAR MDB2::quoteIdentifier() method from
     * http://bit.ly/2NQltfI. Values for quoting identifiers for different dbms
     * are adapted from https://bit.ly/2II5Llg.
     *
     * @param string $identifier
     * @return string
     */
    public function quoteIdentifier($identifier)
    {
        $dbms = $this->params()->dbms();
        $quotes = null;
        switch ($dbms) {
            case 'mysql':
            case 'mysqli':
                $quotes = ['`', '`', '`'];
                break;
            case 'mssql':
            case 'odbc':
            case 'sqlsrv':
                $quotes = ['[', ']', ']'];
                break;
            case 'ibase':
                $quotes = ['"', '"', false];
                break;
            case 'oci8':
            case 'pgsql':
            case 'sqlite':
                $quotes = ['"', '"', '"'];
                break;
            default:
                // Can't occur
                break;
        }
        list($begin, $end, $escape) = $quotes;
        return $begin . str_replace($end, $escape . $end, $identifier) . $end;
    }

    /**
     * Returns a database connection, establishing one if necessary.
     *
     * @return PDO
     * @throws CodeRage\Error
     */
    public function connection()
    {
        if (!$this->connection) {
            if ( $this->cacheId !== null && $this->useCache &&
                 isset(self::$connectionCache[$this->cacheId]) )
            {
                $this->connection = self::$connectionCache[$this->cacheId];
            } else {
                try {
                    $options =
                        [
                            'host' => $this->params->host(),
                            'port' => $this->params->port(),
                            'dbname' => $this->params->database()
                        ];
                    $dsn = self::DBMS_MAPPING[$this->params->dbms()] . ':';
                    foreach ($options as $n => $v)
                        if ($v !== null)
                            $dsn .= "$n=$v;";
                    $driverOptions =
                        [
                            // Throw exception on error
                            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,

                            // Disable emulation of prepared statements if
                            // native prepared statements are supported
                            PDO::ATTR_EMULATE_PREPARES => false,

                            // Return string values by default
                            PDO::ATTR_STRINGIFY_FETCHES => true
                        ];
                    foreach ($this->params->options() as $n => $v) {
                        if (!defined("PDO::$n"))
                            throw new
                                Error([
                                    'status' => 'DATABASE_ERROR',
                                    'details' =>
                                        "Unsupported connection option: $n"
                                ]);
                        $driverOptions[constant("PDO::$n")] = $v;
                    }
                    $conn =
                        new PDO(
                                $dsn,
                                $this->params->username(),
                                $this->params->password(),
                                $driverOptions
                            );
                    $this->connection = $conn;
                    if ($this->cacheId !== null && $this->useCache)
                        self::$connectionCache[$this->cacheId] = $conn;
                } catch (PDOException $e) {
                    throw new
                        Error([
                            'status' => 'DATABASE_ERROR',
                            'details' => 'Failed connecting to database',
                            'inner' => $e
                        ]);
                }
            }
        }
        return $this->connection;
    }

    /**
     * Closes the underlying database conection.
     */
    public function disconnect()
    {
        if ($this->connection !== null) {
            if ( $this->cacheId != null &&
                 isset(self::$connectionCache[$this->cacheId]) )
            {
                unset(self::$connectionCache[$this->cacheId]);
            }
            $this->connection = null;
        }
    }

    /**
     * Registers a hook
     *
     * @param array $options The options array; supports the following options:
     *     preQuery - A callable with the signature
     *       preQuery(CodeRage\Db\Hook $hook, string $sql) (optional)
     *     postQuery - A callable with the signature
     *       postQuery(CodeRage\Db\Hook $hook, string $sql) (optional)
     *
     * @return CodeRage\Db\Hook
     */
    public function registerHook(array $options)
    {
        $conn = $this->connection();
        if (self::$hooks === null)
            self::$hooks = new \SplObjectStorage;
        if (!isset($hooks[$conn]))
            self::$hooks[$conn] = new \stdClass;
        $h = new Hook($options + ['db' => $this]);
        self::$hooks[$conn]->{$h->id()} = $h;
        return $h;
    }

    /**
     * Unregisters a hook
     *
     * @param CodeRage\Db\Hook $hook A hook returned by a prior call to
     *   registerHook() on this instance
     */
    public function unregisterHook(Hook $hook)
    {
        if (self::$hooks === null || $this->connection === null)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'The specified hook is not registered'
                ]);
        $conn = $this->connection;
        $other = self::$hooks[$conn]->{$hook->id()} ?? null;
        if ($other !== $hook)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => 'The specified hook is not registered'
                ]);
        unset(self::$hooks[$conn]->{$hook->id()});
    }

    /**
     * Returns an instance which does not support nested transactions
     *
     * @return CodeRage\Db
     */
    public static function nonNestableInstance()
    {
        static $instance;
        if ($instance === null) {
            $instance = new Db(['useCache' => false]);
            $instance->nestable = false;
        }
        return $instance;
    }

    /**
     * Returns a static data structure mapping PDO objects to simulated
     * transaction depth
     *
     * @return SplObjectStorage
     */
    private static function transactionDepth()
    {
        static $transactionDepth;
        if ($transactionDepth === null)
            $transactionDepth = new \SplObjectStorage;
        return $transactionDepth;
    }

    /**
     * Returns connection parameters for the named data source
     *
     * @param string $name The data source name
     * @return CodeRage\Db\Params
     */
    private static function loadNamedDataSource($name)
    {
        if (!isset(self::$paramsCache[$name])) {
            $json = null;
            if (ctype_alnum($name)) {
                $path = Config::projectRoot() . "/.coderage/db/$name.json";
                File::checkFile($path, 0b0100);
                $json = file_get_contents($path);
            } else {
                $json = $name;
            }
            $options = json_decode($json, true);
            if ($options === null)
                throw new
                    Error([
                        'status' => 'CONFIGURATION_ERROR',
                        'details' =>
                            "JSON decoding error for named data source '$name'"
                    ]);
            Args::check($options, 'map', "named data source '$name'");
            self::$paramsCache[$name] = new Params($options);
        }
        return self::$paramsCache[$name];
    }

    /**
     * Returns a placeholder to which the given value can be bound in a SQL
     * query
     *
     * @param string $value A scalar value
     * @return string
     */
    private static function placeholder($value)
    {
        return is_int($value) || is_bool($value) ?
            '%i' :
            ( is_float($value) ?
                  '%f' :
                  '%s' );
    }

    /**
     * Returns a string for use as an key in $paramsCache
     *
     * @param CodeRage\Sys\ProjectConfig $config
     * @return string
     */
    private static function cacheId(\CodeRage\Sys\ProjectConfig $config)
    {
        if ($config instanceof \CodeRage\Sys\Config\Builtin) {
            return '';
        } else {
            $props = [];
            foreach ($config->propertyNames() as $n)
                if (strncmp($n, 'db.', 3) == 0)
                    $props[$n] = $config->getProperty($n);
            return json_encode($props);
        }
     }

    /**
     * An associative array mapping data source names to instances of
     * CodeRage\Db\Param
     *
     * @var array
     */
    private static $paramsCache = [];

    /**
     * An associative array mapping data source names to instances of PDO
     *
     * @var array
     */
    private static $connectionCache = [];

    /**
     * Maps PDO objects to collections of instances of CodeRage\Db\Hook,
     * indexed by hook ID
     *
     * @var SplObjectStorage
     */
    private static $hooks;

    /**
     * The connection parameters
     *
     * @var CodeRage\Db\Params
     */
    private $params;

    /**
     * The database connection, if any
     *
     * @var PDO
     */
    private $connection;

    /**
     * true if a cached connection should be used
     *
     * @var boolean
     */
    private $useCache;

    /**
     * true if simulated nested trasactions are supported
     *
     * @var boolean
     */
    private $nestable;

    /**
     * @var CodeRage\Db\QueryProcessor
     */
    private $queryProcessor;

    /**
     * An integer used as an index into the parameters and connections caches
     *
     * @var int
     */
    private $cacheId;
}
