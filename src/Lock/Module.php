<?php

/**
 * Defines the class CodeRage\Lock\Module
 *
 * File:        CodeRage/Lock/Module.php
 * Date:        Thu Apr 29 21:52:12 UTC 2021
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2021 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Lock;

use CodeRage\Sys\Engine;

/**
 * Lock module
 */
final class Module extends \CodeRage\Sys\BasicModule {

    /**
     * Constructs an instance of CodeRage\Lock\Module
     *
     * @param array $options The options array
     */
    public function __construct(array $options)
    {
        parent::__construct([
            'title' => 'Lock',
            'description' => 'Lock module',
            'dependencies' => ['CodeRage.Db.Module'],
            'tables' => [__DIR__ . '/lock.tbx']
        ]);
    }
}
