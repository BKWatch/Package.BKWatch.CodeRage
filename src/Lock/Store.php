<?php

/**
 * Defines the class CodeRage\Lock\Store
 *
 * File:        CodeRage/Lock/Store.php
 * Date:        Thu Apr 29 21:52:12 UTC 2021
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2021 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Lock;

use CodeRage\Db;
use CodeRage\Sys\Engine;
use CodeRage\Util\Args;

/**
 * Implementation of Symfony\Component\Lock\Store\PersistingStoreInterface
 */
final class Store extends \Symfony\Component\Lock\Store\PdoStore {

    /**
     * @var array
     */
    private const PDO_STORE_OPTIONS =
        [
            'db_table' => 'LockKey',
            'db_id_col' => 'id',
            'db_token_col' => 'token',
            'db_expiration_col' => 'expiration'
        ];

    /**
     * Constructs an instance of CodeRage\Lock\Store. Establishes a database
     * connection if one is not already available; designed to be constructed
     * only when needed.
     *
     * @param array $options The options array; supports the following options:
     *     gcProbability - Probability expressed as floating number between 0
     *         and 1 to clean old locks
     *     initialTtl - The expiration delay of locks in seconds
     */
    public function __construct(array $options = [])
    {
        $gcProbability = Args::checkKey($options, 'gcProbability', 'number');
        $initialTtl = Args::checkKey($options, 'initialTtl', 'int');
        if (($gcProbability === null) != ($initialTtl === null)) {
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        "The options 'gcProbability' and 'initialTtl' must " .
                        "be specified together"
                ]);
        }
        $conn = Db::nonNestableInstance()->connection();
        $args = [$conn, self::PDO_STORE_OPTIONS];
        if ($gcProbability !== null) {
            $args[] = $gcProbability;
            $args[] = $initialTtl;
        }
        parent::__construct(...$args);
    }
}
