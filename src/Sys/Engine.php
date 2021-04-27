<?php

/**
 * Defines the class CodeRage\Sys\Engine
 *
 * File:        CodeRage/Sys/Engine.php
 * Date:        Thu Dec 10 23:30:32 UTC 2020
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2015 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Sys;

use Throwable;
use CodeRage\Sys\Config\Reader\Array_ as ArrayReader;
use CodeRage\Sys\Config\Reader\File as FileReader;
use CodeRage\Error;
use CodeRage\File;
use CodeRage\Log;
use CodeRage\Text;
use CodeRage\Util\Args;
use CodeRage\Util\Time;
use CodeRage\Xml;

/**
 * Executes build operations
 */
class Engine extends \CodeRage\Util\BasicProperties {

    /**
     * @var string
     */
    public const CONFIG_FILE = 'project.xml';

    /**
     * The three modes of operation
     *
     * @var int
     */
    private const MODES = ['build' => 1, 'install' => 1, 'run' => 1];

    /**
     * The default mode
     *
     * @var int
     */
    private const DEFAULT_MODE = 'run';

    /**
     * The default value of the "logErrorCount" option
     *
     * @var int
     */
    private const DEFAULT_UPDATE_CONFIG =
        ['build' => true, 'install' => true, 'run' => false];

    /**
     * The default value of the "logErrorCount" option
     *
     * @var int
     */
    private const DEFAULT_LOG_ERROR_COUNT =
        ['build' => true, 'install' => true, 'run' => false];

    /**
     * The default log level for console logging.
     *
     * @var int
     */
    private const LOG_CONSOLE_LEVEL = Log::INFO;

    /**
     * Value of the 'stream' parameter to the CodeRage\Log\Provider\Console
     * constructor
     *
     * @var int
     */
    private const LOG_CONSOLE_STREAM = 'stderr';

    /**
     * Value of the 'format' parameter to the CodeRage\Log\Provider\Console
     * constructor
     *
     * @var int
     */
    private const LOG_CONSOLE_FORMAT = 0;

    /**
     * The log level for entry counting.
     *
     * @var int
     */
    private const LOG_COUNTER_LEVEL = Log::WARNING;

    /**
     * Path, relative to the project root directory, of the list of generated
     * files
     *
     * @var string
     */
    private const GENERATED_FILE_LOG = '.coderage/files.log';

    /**
     * Constructs an instance of CodeRage\Sys\Engine
     *
     * @param array $options The options array; supports the following options:
     *     log - An instance of CodeRage\Log (optional)
     */
    public function __construct(array $options = [])
    {
        $this->projectRoot = \CodeRage\Config::projectRoot();
        $this->log = Args::checkKey($options, 'log', 'CodeRage\Log');
    }

    /**
     * Returns the mode, during engine execution, and null otherwise
     *
     * @return string
     */
    public final function mode() : ?string
    {
        return $this->mode;
    }

    /**
     * Return the log, if any
     *
     * @return CodeRage\Log
     */
    public final function log() : ?Log
    {
        return $this->log;
    }

    /**
     * Return the project configuratio. The returned instance will be an
     * instance of CodeRage\Sys\ProjectConfig if the current mode is "build" or
     * "install"
     *
     * @return CodeRage\Sys\ProjectConfig
     */
    public final function projectConfig() : ?ProjectConfig
    {
        return $this->projectConfig;
    }

    /**
     * Return the collection of modules
     *
     * @return CodeRage\Sys\ModuleStore
     */
    public final function moduleStore() : ?ModuleStore
    {
        return $this->moduleStore;
    }

    /**
     * Returns the dependency injection container
     *
     * @return Psr\Container\ContainerInterface
     */
    public final function container() : \Psr\Container\ContainerInterface
    {
        if ($this->container === null) {
            if ($this->moduleStore === null) {
                throw new
                    Error([
                        'status' => 'STATE_ERROR',
                        'message' => 'Module system not initialized'
                    ]);
            }
            $module = $this->moduleStore->lookupModule('CodeRage.Sys.Module.Container');
            return $module->loadContainer($this);
        }
        return $this->container;
    }

    /**
     * Stores $path in the list of generated files.
     *
     * @param string $path
     */
    public final function recordGeneratedFile($path)
    {
        if ($str = $this->log->getStream(Log::DEBUG))
            $str->write("Recording generated file: $path");
        if (!file_exists($path))
            throw new
                Error([
                    'message' =>
                        "Failed recording generated file: no such file '$path'"
                ]);
        if (!File::isAbsolute($path))
            throw new
                Error([
                    'message' =>
                        "Failed recording generated file: expected absolute " .
                        "path; found $path"
                ]);
        $this->files[] = realpath($path);
    }

    /**
     * Executes the build event "clean": removes generated files
     *
     * @return boolean
     */
    public final function clean()
    {
        return
            $this->run(function($engine) {
                $engine->cleanImpl();
            }, [
                'mode' => 'build',
                'updateConfig' => false,
                'throwOnError' => false
            ]);
    }

    /**
     * Executes the build event "reset": removes all build artifacts, including
     * the build history and any configuration variables specified on the
     * command line
     *
     * @return boolean
     */
    public final function reset()
    {
        return
            $this->run(function($engine) {
                $engine->cleanImpl();
                File::rm($this->projectRoot . '/.coderage');
            }, [
                'mode' => 'build',
                'updateConfig' => false,
                'throwOnError' => false
            ]);
    }

    /**
     * Executes the build event "build"
     *
     * @param array $options The options array; supports the following options"
     *     setProperties - An associative array of string-valued configuration
     *       variables to set (optional)
     *     unsetProperties - A list of names of configuration variables to unset
     *       (optional)
     * @return boolean
     */
    public final function build(array $options = [])
    {
        foreach ($options as $name => $value) {
            if ($name != 'setProperties' && $name != 'unsetProperties') {
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "Upsupported option: $name"
                    ]);
            }
        }
        $options['mode'] = 'build';
        $options['throwOnError'] = 'false';
        self::processOptions($options);
        return $this->run(function($engine) {
            $engine->foreachModule('build');
        }, $options);
    }

    /**
     * Executes the build event "install"
     *
     * @return boolean
     */
    public final function install()
    {
        return $this->run(function($engine) {
            $engine->foreachModule('install');
        }, [
            'mode' => 'build',
            'throwOnError' => false
        ]);
    }

    /**
     * Executes the build event "sync"
     *
     * @return boolean
     */
    public final function sync()
    {
        return $this->run(function($engine) {
            $engine->foreachModule('sync');
        }, [
            'throwOnError' => false
        ]);
    }

    /**
     * Executes a build action
     *
     * @param callable $action The build action, as a callable taking two
     *   arguments of types CodeRage\Sys\Engine and array
     * @param array $options The options array; supports the following opt
     *     mode - One of "build", "install", or "run"; defaults to "run"
     *     setProperties - An associative array of string-valued configuration
     *       variables to set (optional)
     *     unsetProperties - A list of names of configuration variables to unset
     *       (optional):
     *     updateConfig - true to update the configuration; defaults to false if
     *       mode is "run" and true otherwise
     *     logErrorCount - true to log the error count; defaults to false if
     *       mode is "run" and true otherwise
     *     setAsCurrent - true to register this instances as the global engine,
     *       accessible via current(), for the duration of this method; defaults
     *       to true
     *     throwOnError - true to rethrow any exception that occurs during
     *       execution; defaults to true
     * @return bool
     */
    public final function run(callable $action, array $options = []) : bool
    {
        try {
            \CodeRage\Util\ErrorHandler::register();

            // Process options
            $this->processOptions($options);

            // Initialize state
            $this->mode = $options['mode'];
            if ($this->log === null) {
                $this->log = $this->mode == 'run' ?
                    Log::Current() :
                    new Log;
            }
            $this->projectConfig = $this->moduleStore = null;
            $this->files = [];

            // Add counter to log
            $logCount = $options['logErrorCount'];
            $counter = null;
            if ($logCount) {
                $counter = new \CodeRage\Log\Provider\Counter;
                $this->log->registerProvider($counter, self::LOG_COUNTER_LEVEL);
            }

            // Perform main work
            $status = true;
            $current = self::$current;
            try {
                $this->projectConfig = $this->mode == 'run' ?
                    new Config\Builtin :
                    $this->loadProjectConfig();
                $this->moduleStore = ModuleStore::load($this);
                if ($options['updateConfig'])
                    $this->updateConfig($options);
                if ($options['setAsCurrent'])
                    self::$current = $this;
                $action($this);
            } catch (Throwable $e) {
                if ($options['throwOnError']) {
                    throw $e;
                } else {
                    $status = false;
                    $this->log->logError($e);
                }
            } finally {
                self::$current = $current;

                // Clear state
                $this->mode = $this->projectConfig = $this->moduleStore = null;
                $this->files = [];

                // Remove counter
                if ($counter !== null)
                    $this->log->unregisterProvider($counter);
            }

            if ($logCount) {
                $count = $counter->getCount();
                $this->log->logMessage("There were $count error(s)");
            }

            return $status;
        } finally {
            restore_error_handler();
        }
    }

    /**
     * Returns the global engine, if any
     *
     * @return CodeRage\Sys\Engine
     */
    public static function current(): ?self
    {
        return self::$current;
    }

    /**
     * Validates and processes options for run()
     *
     * @param array $options The options array passed to run()
     */
    private function processOptions(array &$options) : void
    {
        Args::checkKey($options, 'setProperties', 'map[string]', [
            'default' => []
        ]);
        Args::checkKey($options, 'unsetProperties', 'list[string]', [
            'default' => []
        ]);
        $mode =
            Args::checkKey($options, 'mode', 'string', [
                'default' => self::DEFAULT_MODE
            ]);
        if (!array_key_exists($mode, self::MODES)) {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' => "Invalid mode: $mode"
                ]);
        }
        $updateConfig =
            Args::checkBooleanKey($options, 'updateConfig', [
                'default' => self::DEFAULT_UPDATE_CONFIG[$mode]
            ]);
        if ($updateConfig && $mode == 'run') {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' =>
                        'Updating the project configuration is not supported ' .
                        'at runtime'
                ]);
        }
        Args::checkBooleanKey($options, 'logErrorCount', [
            'default' => self::DEFAULT_LOG_ERROR_COUNT[$mode]
        ]);
        Args::checkBooleanKey($options, 'setAsCurrent', [
            'default' => true
        ]);
        Args::checkBooleanKey($options, 'throwOnError', [
            'default' => true
        ]);
    }

    /**
     * Removes generated files
     */
    private function cleanImpl() : void
    {
        foreach ($this->moduleStore->modules() as $module) {
            $this->log->logMessage('Processing module ' . $module->name());
            $module->clean($this);
        }
        $this->log->logMessage('Removing generated files');
        $path = $this->projectRoot . '/' . self::GENERATED_FILE_LOG;
        if (!file_exists($path))
            return;
        File::checkFile($path, 0b0110);
        $files = explode("\n", rtrim(file_get_contents($path)));
        for ($z = sizeof($files) - 1; $z != -1; --$z) {
            $f = $files[$z];
            try {
                File::rm($f);
                array_splice($files, $z, 1);
            } catch (Throwable $e) {
                $this->log->logError("Failed removing file: $e");
            }
        }
        if (count($files)) {
            file_put_contents($path, join("\n", $files));
        } else {
            File::rm($path);
        }
    }

    /**
     * Helper method for config(), build(), install(), and sync()
     *
     * @param string $event One of build, install, or sync
     * @param array $options The options array passed to execute(), after
     *   processing
     */
    private function foreachModule(string $event) : void
    {
        try {
            foreach ($this->moduleStore->modules() as $module) {
                $this->log->logMessage('Processing module ' . $module->name());
                $module->$event($this);
            }
        } finally {
            $this->recordGeneratedFiles();
        }
    }

    /**
     * Updates the build parameters and project configuration
     *
     * @param array $options The options array
     */
    private function updateConfig(array $options) : void
    {
        if ($str = $this->log->getStream(Log::INFO))
            $str->write("Updating project configuration");

        // Update configuration
        $this->projectConfig = $this->generateProjectConfig($options);
        $this->moduleStore->update();

        // Store configuration
        foreach (['xml', 'php'] as $lang) {
            if ($str = $this->log->getStream(Log::INFO))
                $str->write("Generating $lang configuration");
            $class = 'CodeRage\\Sys\\Config\\Writer\\' . ucfirst($lang);
            $writer = new $class;
            $path = "$this->projectRoot/.coderage/config.$lang";
            $writer->write($this->projectConfig, $path);
        }
        $this->moduleStore->save();
    }

    /**
     * Returns an instance of CodeRage\Sys\ProjectConfig constructed from the
     * given build parameters
     *
     * @param CodeRage\Sys\BuildParams $new The new build parameters
     * @param array $options The options array
     * @return CodeRage\Sys\ProjectConfig
     */
    private function generateProjectConfig(array $options)
        : ProjectConfig
    {
        if ($str = $this->log->getStream(Log::INFO))
            $str->write("Generating project configuration");

        $configs = [];

        // Handle CodeRage project definition file
        $reader = new FileReader(dirname(__DIR__) . '/' . self::CONFIG_FILE);
        $configs[] = $reader->read();

        // Handle module configurations
        foreach ($this->moduleStore->modules() as $module) {
            if ($path = $module->configFile()) {
                if ($str = $this->log->getStream(Log::VERBOSE)) {
                    $str->write("Processing configuration file $path");
                }
                $reader = new FileReader($path, '[' . $module->name() . ']');
                $configs[] = $reader->read();
            }
            if (($config = $module->config()) !== null) {
                if ($str = $this->log->getStream(Log::VERBOSE)) {
                    $str->write(
                        'Processing configuration for module ' . $module->name()
                    );
                }
                $reader = new ArrayReader($config, '[' . $module->name() . ']');
                $configs[] = $reader->read();
            }
        }

        // Handle project definition file
        $path = \CodeRage\Config::projectRoot() . '/' . self::CONFIG_FILE;
        $reader = new FileReader($path);
        $configs[] = $reader->read();

        // Handle previous command-line
        $prev = $this->loadProjectConfig();
        $cmdline = new Config\Basic;
        foreach ($prev->propertyNames() as $n) {
            $p = $prev->lookupProperty($n);
            if ( $p->setAt() == '[cli]' &&
                 !in_array($n, $options['unsetProperties']) )
            {
                $cmdline->addProperty($n, $p);
            }
        }
        $configs[] = $cmdline;

        // Handle current command-line
        $cmdline = new Config\Basic;
        foreach ($options['setProperties'] as $n => $v) {
            if (!in_array($n, $options['unsetProperties'])) {
                $cmdline->addProperty($n, Property::decode($v, '[cli]'));
            }
        }
        $configs[] = $cmdline;

        // Construct configuration
        $result =  new Config\Compound(array_reverse($configs));

        return $result;
    }

    /**
     * Returns the existing project configuration, if any, and and empty
     * configuration otherwise
     *
     * @return CodeRage\Sys\ProjectConfig
     */
    private function loadProjectConfig() : ProjectConfig
    {
        $this->log->logMessage("Loading project configuration");
        $file = "$this->projectRoot/.coderage/config.xml";
        if (file_exists($file)) {
            $reader = new FileReader($file);
            return $reader->read();
        } else {
            return new Config\Basic;
        }
    }

    /**
     * Stores the pathnames of files generates during the current execution
     */
    private function recordGeneratedFiles()
    {
        if (sizeof($this->files) == 0)
            return;
        $path = $this->projectRoot .'/' . self::GENERATED_FILE_LOG;
        if (!file_exists($path)) {
            File::checkDirectory(dirname($path), 0b0111);
            touch($path);
        }
        $path = realpath($path);
        if ($str = $this->log->getStream(Log::VERBOSE))
            $str->write("Saving list of generated files to '$path'");
        $content = file_get_contents($path);
        foreach (explode("\n", rtrim($content)) as $f)
            $this->files[] = $f;
        $this->files = array_unique($this->files);
        $content = join("\n", $this->files);
        file_put_contents($path, $content);
    }

    /**
     * The global engine, if any
     *
     * @var CodeRage\Sys\Engine
     */
    static private $current;

    /**
     * The log
     *
     * @var CodeRage\Log
     */
    private $log;

    /**
     * The path to the project root
     *
     * @var string
     */
    private $projectRoot;

    /**
     * The project configuration.
     *
     * @var CodeRage\Sys\ProjectConfig
     */
    private $projectConfig;

    /**
     * The collection of modules
     *
     * @var CodeRage\Sys\ModuleStore
     */
    private $moduleStore;

    /**
     * The dependency injection container
     *
     * @var Psr\Container\ContainerInterface
     */
    private $container;

    /**
     * The list of generated files
     *
     * @var array
     */
    private $files = [];
}
