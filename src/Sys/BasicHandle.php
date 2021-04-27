<?php

/**
 * Defines the class CodeRage\Sys\BasicHandle
 *
 * File:        CodeRage/Sys/BasicHandle.php
 * Date:        Tue Feb  7 06:03:23 UTC 2017
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Sys;

use Psr\Container\ContainerInterface;
use CodeRage\Access\Session;
use CodeRage\Db;
use CodeRage\Error;
use CodeRage\Log;
use CodeRage\Util\Args;

/**
 * Basic implementation of CodeRage\Sys\Handle
 */
class BasicHandle implements Handle {

    /**
     * Constructs an instance of CodeRage\Sys\BasicHandle
     *
     * @param array $options The options array; supports the following options:
     *     engine - An instance of CodeRage\Sys\Engine (optional)
     *     container - An instance of Psr\Container\ContainerInterface (optional)
     *     handle - An instance of CodeRage\Sys\Handle (optional)
     *   At most one option must be supplied
     */
    public function __construct($options = [])
    {
        $engine = Args::checkKey($options, 'engine', 'CodeRage\Sys\Engine');
        $container = Args::checkKey($options, 'container', 'Psr\Container\ContainerInterface');
        $handle = Args::checkKey($options, 'handle', 'CodeRage\Sys\Handle');
        $count = ($engine !== null) + ($container !== null) + ($handle !== null);
        if ($count > 1) {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "At most one of the options 'engine', 'container', " .
                        "and 'handle' may be supplied"
                ]);
        } elseif ($count == 0) {
            $container = self::constructContainer();
        }
        $this->container = $container !== null ?
            $container :
            ($engine !== null ? $engine->container() : $handle->container());
        $this->session = null;
        $this->logTagged = false;
    }

    public function container(): ContainerInterface
    {
        return $this->container;
    }

    public function config(): \CodeRage\Sys\ProjectConfig
    {
        return $this->container->get('config');
    }

    public function db(): Db
    {
        return $this->container->get('db');
    }

    public function log(): Log
    {
        $log = $this->container->get('log');
        if (!$this->logTagged) {
            $log->setTag(str_replace('\\', '.', get_class($this)));
            $this->logTagged = true;
        }
        return $log;
    }

    public function session(): ?Session
    {
        if ($this->session === null) {
            $this->session = Session::current();
        }
        return $this->session;
    }

    public function setSession(Session $session): void
    {
        $this->session = $session;
    }

    public function hasService(string $service): bool
    {
        return $this->container()->has($service);
    }

    public function getService(string $service)
    {
        return $this->container()->get($service);
    }
    public final function logMessage($msg): void
    {
        if ($log = $this->log()->getStream(Log::INFO))
            $log->write($msg);
    }

    public final function logWarning($msg): void
    {
        if ($log = $this->log()->getStream(Log::WARNING))
            $log->write($msg);
    }

    public final function logError($msg): void
    {
        if ($log = $this->log()->getStream(Log::ERROR))
            $log->write($msg);
    }

    public final function logCritical($msg): void
    {
        if ($log = $this->log()->getStream(Log::CRITICAL))
            $log->write($msg);
    }

    /**
     * Helper for the constructor
     *
     * @return Psr\Container\ContainerInterface
     */
    private static function constructContainer(): ContainerInterface
    {
        $container = new \CodeRage\Util\Container;
        $container->add([
            'name' => 'config',
            'service' =>  function() { return \CodeRage\Config::current(); }
        ]);
        $container->add([
            'name' => 'db',
            'service' =>  Db::class
        ]);
        $container->add([
            'name' => 'log',
            'service' =>  function() { return Log::current(); }
        ]);
        return $container;
    }

    /**
     * @var Psr\Container\ContainerInterface
     */
    private $container;

    /**
     * @var Pacer\Session
     */
    private $session;

    /**
     * @var boolean
     */
    private $logTagged;
}
