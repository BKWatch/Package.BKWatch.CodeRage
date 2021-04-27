<?php

/**
 * Defines the class CodeRage\Sys\Command\Info
 *
 * File:        CodeRage/Sys/Command/Info.php
 * Date:        Thu Jan 24 13:39:40 MST 2008
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2021 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Sys\Command;

use CodeRage\Error;

/**
 * Implements "crush info"
 */
final class Info extends Base {

    /**
     * @var integer
     */
    private const MATCH_FILTER = '#^\*?[._a-zA-Z0-9]+(\*[._a-zA-Z0-9]+)*\*?$#';

    /**
     * @var integer
     */
    private const MAX_NAME_LENGTH = 45;

    /**
     * @var integer
     */
    private const MAX_VALUE_LENGTH = 90;

    /**
     * Constructs an instance of CodeRage\Sys\Command\Sync
     */
    public function __construct()
    {
        parent::__construct([
            'name' => 'info',
            'description' =>
                'Displays configuration information about a project'
        ]);
        $this->addOption([
            'shortForm' => 'm',
            'longForm' => 'modules',
            'description' => 'List modules',
            'type' => 'switch'
        ]);
        $this->addOption([
            'shortForm' => 'r',
            'longForm' => 'raw',
            'description' =>
                'Display the raw value of the variables instead of the ' .
                'calculated value',
            'type' => 'switch'
        ]);
        $this->addOption([
            'shortForm' => 'f',
            'longForm' => 'filter',
            'description' =>
                'Display only configuration variables with names matching ' .
                'wildcard expression <<PATTERN>>'
        ]);
        $this->addOption([
            'shortForm' => 'g',
            'longForm' => 'group',
            'description' =>
                'Group configuration variables according to where they were set',
            'type' => 'switch'
        ]);
        $this->addOption([
            'longForm' => 'no-clip',
            'description' => "Don't clip configuration long variable values",
            'type' => 'switch'
        ]);
        $this->addOption([
            'longForm' => 'config-command',
            'description' =>
                'Display a command that can be used to reconfigure the ' .
                'project after it is reset',
            'type' => 'switch'
        ]);
    }

    protected function doExecute()
    {
        $this->validateCommandLine();
        $engine = $this->createEngine(['defaultLogLevel' => \CodeRage\Log::WARNING]);
        return $engine->run(function($engine) {
            return $this->executeImpl($engine);
        }, [
            'mode' => 'build',
            'updateConfig' => false,
            'throwOnError' => false
        ]);
    }

    /**
     * Throws an exception if the command-line options are inconsistent
     *
     * @throws CodeRage\Error
     */
    private function validateCommandLine()
    {
        foreach (['modules', 'config-command'] as $opt) {
            if ( $this->getValue($opt) &&
                 ( $this->getValue('raw') ||
                   $this->getValue('filter') ||
                   $this->getValue('group') ) )
            {
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' =>
                            "The option --$opt may not be combined with " .
                            "other options"
                    ]);
            }
        }
        if ($this->hasValue('filter')) {
            $filter = $this->getValue('filter');
            if (!preg_match(self::MATCH_FILTER, $filter)) {
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "Invalid wildcard expression: $filter"
                    ]);
            }
        }
    }

    /**
     * Helper for doExecute()
     *
     * @return bool
     */
    private function executeImpl(\CodeRage\Sys\Engine $engine): bool
    {
        // Handle --modules
        if ($this->getValue('modules')) {
            $result = '';
            foreach ($engine->moduleStore()->modules() as $m) {
                $result .= $m->name() . PHP_EOL;
            }
            echo $result;
            return true;
        }

        $raw = $this->getValue('raw') || $this->getValue('config-command');
        $filter = $this->hasValue('filter') ?
            $this->wildcardToRegex($this->getValue('filter')) :
            null;
        $group = $this->getValue('group') || $this->getValue('config-command');

        // Collect configuration variables
        $config = $engine->projectConfig();
        $props = [];
        foreach ($config->propertyNames() as $name) {
            if ($filter && !preg_match($filter, $name)) {
                continue;
            }
            $prop = $config->lookupProperty($name);
            $value = $raw ? $prop->encode() : $prop->evaluate();
            if ($group) {
                $props[$prop->setAt()][$name] = $value;
            } else {
                $props[$name] = $value;
            }
        }
        if ($group) {
            foreach ($props as $location => $ignore) {
                ksort($props[$location]);
            }
        } else {
            ksort($props);
        }

        // Construct output:
        $result = null;
        if ($this->getValue('config-command')) {
            if (!isset($props['[cli]'])) {
                echo "No CLI configuration variables are set" . PHP_EOL;
                return true;
            }
            $result = 'crush config';
            foreach ($props['[cli]'] as $name => $value) {
                $result .= " --set $name=" . escapeshellarg($value);
            }
        } elseif ($group) {
            $result = '';
            foreach ($props as $location => $values) {
                $result .= "$location:" . PHP_EOL;
                foreach ($values as $name => $value) {
                    $result .= '  ' .
                        str_pad($name, self::MAX_NAME_LENGTH, ' ');
                    $result .= $this->formatString($value) . PHP_EOL;
                }
            }
        } else {
            $result = '';
            foreach ($props as $name => $value) {
                $result .= str_pad($name, self::MAX_NAME_LENGTH, ' ');
                $result .= $this->formatString($value) . PHP_EOL;
            }
        }

        echo "$result";
        return true;
    }

    /**
     * Translates the given wildcard expression to a regular expression
     *
     * @param string $value
     * @return string
     */
    private function wildcardToRegex(string $filter): string
    {
        return '/^' . str_replace('*', '.*', $filter). '$/';
    }

    /**
     * Returns a PHP expression evaluating to the given string
     *
     * @param string $value
     * @return string
     */
    private function formatString(string $value): string
    {
        if (!$this->getValue('no-clip') && strlen($value) > self::MAX_VALUE_LENGTH) {
            $value = substr($value, 0, self::MAX_VALUE_LENGTH) . '...';
        }
        return strlen($value) == 0 || ctype_print($value) ?
            addcslashes($value, "\\'"):
            "base64_decode('" . base64_encode($value) . "')";
    }
}
