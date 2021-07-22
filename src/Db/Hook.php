<?php

/**
 * Defines the class CodeRage\Db\Hook
 *
 * File:        CodeRage/Db/Hook.php
 * Date:        Wed Nov  6 20:22:12 UTC 2019
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2018 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Db;

use CodeRage\Db;
use CodeRage\Util\Args;


/**
 * Container for callbacks invoked at various points during query execution
 */
class Hook {

    /**
     * Constructs a CodeRage\Db\Hooks
     *
     * @param array $options; the options array; supports the following options:
     *     preQuery - A callable with the signature
     *       preQuery(CodeRage\Db\Hook $hook, string $sql) (optional)
     *     postQuery - A callable with the signature
     *       postQuery(CodeRage\Db\Hook $hook, string $sql) (optional)
     */
    public function __construct(array $options = [])
    {
        $this->preQuery = Args::checkKey($options, 'preQuery', 'callable');
        $this->postQuery = Args::checkKey($options, 'postQuery', 'callable');
    }

    /**
     * Returns a reference to an array used to store data accumulated during
     * query execution
     */
    public final function &data()
    {
        return $this->data;
    }

    /**
     * Called immediately before the given query is executed
     *
     * @param string $sql
     */
    public function preQuery($sql)
    {
        if ($this->preQuery !== null)
            ($this->preQuery)($this, $sql);
    }

    /**
     * Called immediately after  the given query is executed
     *
     * @param string $sql
     */
    public function postQuery(string $sql)
    {
        if ($this->postQuery !== null)
            ($this->postQuery)($this, $sql);
    }

    /**
     * @var callable
     */
    private $preQuery;

    /**
     * @var callable
     */
    private $postQuery;

    /**
     * An array for use by preQuery() and postQuery()
     *
     * @var array
     */
    private $data = [];
}
