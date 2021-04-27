<?php

/**
 * Defines the class CodeRage\Util\CommandLine
 *
 * File:        CodeRage/Util/CommandLine.php
 * Date:        Sat Nov 10 17:18:34 MST 2007
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Util;

use Exception;
use Throwable;
use CodeRage\Error;
use CodeRage\Text;
use CodeRage\Util\Args;

/**
 * @ignore
 */

/**
 * Represents a command-line interface
 */
class CommandLine {

    /**
     * Line length for wrapping text.
     *
     * @var int
     */
    const LINE_LENGTH = 70;

    /**
     * Constructs a CodeRage\Util\CommandLine
     *
     * @param mixed $options The options array; supports the following options:
     *     name - The command name, as it is entered on the command line
     *     description - A description of the command
     *     notes - Supplementary information about the command
     *     synopsis - An abstract usage example or list of abstract usage
     *       examples, minus the initial list of command names (optional)
     *     examples - A concrete usage example or list of concreate usage
     *       examples, minus the initial list of command names (optional)
     *     options - An array of instances of CodeRage\Util\CommandLineOption
     *       or of associative arrays to be passed to the
     *       CodeRage\Util\CommandLineOption constructor
     *     subcommands - An array of instances of CodeRage\Util\SubCommand or
     *       of associative arrays to be passed to the CodeRage\Util\SubCommand
     *       constructor
     *     action - A callback taking an instance of
     *       CodeRage\Util\CommandLine as an argument and returning a boolean,
     *       used to implement execute() (optional)
     *     helpless - true to suppress automatic generation of --help option and
     *       help subcommand
     *     version - The version string (optional)
     *     copyright - The copyright notice (optional)
     *     bugEmail - The email address for reporting bugs (optional)
     *     formatter - A callback taking an instance of
     *       CodeRage\Util\CommandLine as an argument and returning a string,
     *       used to implement the  option --help and the help subcommand
     *       (optional)
     *     noEngine - false to execute the command's action within
     *       CodeRage\Sys\Engine::run(); defaults to false
     *  For backward compatibity, the constructor may be called as though it had
     *  the signature:
     *      __construct($nameAndSynopsis, $description = null, $example = null)
     */
    function __construct(...$params)
    {
        $options = null;
        switch (count($params)) {
        case 0:
            $options =
                [
                    'name' => 'command',
                    'synopsis' => '[OPTIONS] ARGS...'
                ];
            break;
        case 1:
            if (is_array($params[0])) {

                // The user has passed an options array as argument 1
                $options = $params[0];
                break;
            }
            // Fall trhough
        case 2:
        case 3:

            // Backward-compatibility: the user has passed a non-array as the
            // first argument and possibly a second argument
            $synopsis = $params[0];
            Args::check($synopsis, 'string', 'synopsis');
            $space = strpos($synopsis, ' ');
            if ($space === false)
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "Missing command name in synopsis"
                    ]);
            $description = isset($params[1]) ?
                $params[1] :
                null;
            if ($description !== null)
                Args::check($description, 'string', 'description');
            $example = isset($params[2]) ?
                $params[2] :
                null;
            if ($example !== null)
                Args::check($synopsis, 'string', 'example');
            $options =
                [
                    'name' => substr($synopsis, 0, $space),
                    'synopsis' => substr($synopsis, $space + 1),
                    'description' => $description,
                    'example' => $example
                ];
            break;
        default:
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' => 'Too many constructor arguments'
                ]);
        }
        static $names =
            [
                'name' => 1,
                'description' => 1,
                'notes' => 1,
                'synopsis' => 1,
                'example' => 1,
                'options' => 1,
                'subcommands' => 1,
                'action' => 1,
                'helpless' => 1,
                'version' => 1,
                'copyright' => 1,
                'bugEmail' => 1,
                'formatter' => 1,
                'noEngine' => 1
            ];
        foreach ($options as $n => $v)
            if (!isset($names[$n]))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "Illegal option: $n"
                    ]);
        Args::checkKey($options, 'name', 'string', null, true);
        if ($options['name'][0] == '-')
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' => "Command names may not begin with '-'"
                ]);
        Args::checkKey($options, 'description', 'string');
        Args::checkKey($options, 'notes', 'string');
        if (isset($options['synopsis']) && is_string($options['synopsis']))
            $options['synopsis'] = [$options['synopsis']];
        Args::checkKey($options, 'synopsis', 'array');
        if (isset($options['examples']) && is_string($options['examples']))
            $options['examples'] = [$options['examples']];
        Args::checkKey($options, 'action', 'callable');
        Args::checkKey($options, 'formatter', 'callable');
        Args::checkKey($options, 'helpless', 'boolean', ['default' => false]);
        Args::checkKey($options, 'options', 'array', ['default' => []]);
        Args::checkKey($options, 'subcommands', 'array', ['default' => []]);
        Args::checkKey($options, 'version', 'string');
        Args::checkKey($options, 'copyright', 'string');
        Args::checkKey($options, 'bugEmail', 'string');
        if (!isset($options['formatter']))
            $options['formatter'] =
                function($command) { return $command->usage(); };
        Args::checkKey($options, 'formatter', 'callable');
        Args::checkKey($options, 'noEngine', 'boolean', ['default' => false]);
        foreach ($names as $n => $v)
            if (isset($options[$n]) && $n != 'options' && $n != 'subcommands')
                $this->$n = $options[$n];
        foreach ($options['options'] as $o)
            $this->addOption($o);
        foreach ($options['subcommands'] as $c)
            $this->addSubCommand($c);
    }

                        /*
                         * Public accessor methods
                         */

    /**
     * Returns the command name, as it is entered on the command line
     *
     * @return string
     */
    public final function name() { return $this->name; }

    /**
     * Returns the description of this command
     *
     * @return string
     */
    public final function description() { return $this->description; }

    /**
     * Sets the description of this command
     *
     * @param string $description
     */
    public final function setDescription($description)
    {
        $this->description = $description;
    }

    /**
     * Returns supplementary information about this command
     *
     * @return string
     */
    public final function notes() { return $this->notes; }

    /**
     * Sets the supplementary information about this command
     *
     * @param string $notes
     */
    public final function setNotes($notes)
    {
        $this->notes = $notes;
    }

    /**
     * Returns a list of abstract usage examples, minus the initial list of
     * command names
     *
     * @return string
     */
    public final function synopsis() { return $this->synopsis; }

    /**
     * Adds an item to the list of abstract usage examples
     *
     * @param string $synopsis An abstract usage example, minus the initial list
     *   of command names
     */
    public final function addSynopsis($synopsis)
    {
        $this->synopsis[] = $synopsis;
    }

    /**
     * Returns a list of concrete usage examples, minus the initial list of
     * command names
     *
     * @return string
     */
    public final function example() { return $this->examples; }

    /**
     * Adds an item to the list of concrete usage examples
     *
     * @param string $example A concrete usage example, minus the initial list
     *   of command names
     */
    public final function addExample($example)
    {
        $this->examples[] = $example;
    }

    /**
     * Returns the callback used to implement doExecute(), if any
     *
     * @return callable A callback taking an instance of
     *   CodeRage\Util\CommandLine as an argument
     */
    public final function action() { return $this->action; }

    /**
     * Sets the callback used to implement doExecute()
     *
     * @param callable $action A callback taking an instance of
     *   CodeRage\Util\CommandLine as an argument
     */
    public final function setAction($action)
    {
        $this->action = $action;
    }

    /**
     * Returns true if automatic generation of the option --help and the help
     * subcommand should be suppressed
     *
     * @return boolean
     */
    public final function helpless() { return $this->helpless; }

    /**
     * Specifies whether automatic generation of the option --help and the help
     * subcommand should be suppressed; may not be called after this command has
     * been parsed or after automatic usage information has been generated
     *
     * @param boolean $helpless
     */
    public final function setHelpless($helpless)
    {
        if ($this->preParsed)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'message' =>
                        'Helplessness may not be modified after a command ' .
                        'line has been parsed or after automatic usage ' .
                        'information has been generated'
                ]);
        $this->helpless = $helpless;
    }

    /**
     * Returns the version string, if any
     *
     * @return string
     */
    public final function version() { return $this->version; }

    /**
     * Sets the version string; may not be called after this command has
     * been parsed or after automatic usage information has been generated
     *
     * @param string $version
     */
    public final function setVersion($version)
    {
        if ($this->preParsed)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'message' =>
                        'Version may may not be modified after a command ' .
                        'line has been parsed or after automatic usage ' .
                        'information has been generated'
                ]);
        $this->version = $version;
    }

    /**
     * Returns the copyright notice, if any
     *
     * @return string
     */
    public final function copyright() { return $this->copyright; }

    /**
     * Sets the copyright notice
     *
     * @param string $copyright
     */
    public final function setCopyright($copyright)
    {
        $this->copyright = $copyright;
    }

    /**
     * Returns the email address for reporting bugs, if any
     *
     * @return string
     */
    public final function bugEmail() { return $this->bugEmail; }

    /**
     * Sets the email address for reporting bugs
     *
     * @param string $bugEmail
     */
    public final function setBugEmail($bugEmail)
    {
        $this->bugEmail = $bugEmail;
    }

    /**
     * Returns the callback used to implement the option --help and the help
     * subcommand
     *
     * @return callable A callback taking an instance of
     *   CodeRage\Util\CommandLine as an argument and returning a string
     */
    public final function formatter() { return $this->formatter; }

    /**
     * Sets the callback used to implement the option --help and the help
     * subcommand
     *
     * @param callable $formatter A callback taking an instance of
     *   CodeRage\Util\CommandLine as an argument and returning a string
     */
    public final function setFormatter($formatter)
    {
        $this->formatter = $formatter;
    }

                        /*
                         * Option management methods
                         */

    /**
     * Returns the underlying list of options
     *
     * @return array
     */
    public final function options()
    {
        return $this->options;
    }

    /**
     * Returns true if an option with the given long or short form exists
     *
     * @param string $option
     * @return boolean
     */
    public final function hasOption($option)
    {
        return strlen($option) > 1 ?
            isset($this->longForms[$option]) :
            isset($this->shortForms[$option]);
    }

    /**
     * Returns the option with the given long or short form
     *
     * @param string $option
     * @return CodeRage\Util\CommandLineOption
     * @throws CodeRage\Error if no such option exists.
     */
    public final function lookupOption($option)
    {
        switch (strlen($option)) {
        case 0:
            throw new
                Error([
                    'status' => 'MISSING_PARAMETER',
                    'message' => 'Missing option name'
                ]);
        case 1:
            if (!isset($this->shortForms[$option]))
                throw new
                    Error([
                        'status' => 'OBJECT_DOES_NOT_EXIST',
                        'message' => "No such option: -$option"
                    ]);
            return $this->shortForms[$option];
        default:
            if (!isset($this->longForms[$option]))
                throw new
                    Error([
                        'status' => 'OBJECT_DOES_NOT_EXIST',
                        'message' => "No such option: --$option"
                    ]);
            return $this->longForms[$option];
        }
    }

    /**
     * Adds a command-line option
     *
     * @param mixed $opt An instance of CodeRage\Util\CommandLineOption or
     *   an associative array to be passed to the
     *   CodeRage\Util\CommandLineOption constructor
     */
    public final function addOption($opt)
    {
        if (is_array($opt))
            $opt = new CommandLineOption($opt);
        Args::check($opt, 'CodeRage\\Util\\CommandLineOption', 'option');
        $long = $short = null;
        if (($long = $opt->longForm()) && isset($this->longForms[$long]))
            throw new
                Error([
                    'status' => 'OBJECT_EXISTS',
                    'message' => "Duplicate option: --$long"
                ]);
        if (($short = $opt->shortForm()) && isset($this->shortForms[$short]))
            throw new
                Error([
                    'status' => 'OBJECT_EXISTS',
                    'message' => "Duplicate option: -$short"
                ]);
        $this->options[] = $opt;
        if ($long)
            $this->longForms[$long] = $opt;
        if ($short)
            $this->shortForms[$short] = $opt;
    }

    /**
     * Adds a flag command-line option
     *
     * @param mixed $properties An associative array to be passed to the
     *   CodeRage\Util\CommandLineOption constructor. The value of 'type' will
     *   be set to 'switch'
     */
    public final function addSwitchOption(array $properties)
    {
        $properties['type'] = 'switch';
        $this->addOption($properties);
    }

    /**
     * Adds a boolean command-line option
     *
     * @param mixed $properties An associative array to be passed to the
     *   CodeRage\Util\CommandLineOption constructor. The value of 'type' will
     *   be set to 'boolean'
     */
    public final function addBooleanOption(array $properties)
    {
        $properties['type'] = 'boolean';
        $this->addOption($properties);
    }

    /**
     * Adds an integral command-line option
     *
     * @param mixed $properties An associative array to be passed to the
     *   CodeRage\Util\CommandLineOption constructor. The value of 'type' will
     *   be set to 'int'
     */
    public final function addIntOption(array $properties)
    {
        $properties['type'] = 'int';
        $this->addOption($properties);
    }

    /**
     * Adds an floating point command-line option
     *
     * @param mixed $properties An associative array to be passed to the
     *   CodeRage\Util\CommandLineOption constructor. The value of 'type' will
     *   be set to 'float'
     */
    public final function addFloatOption(array $properties)
    {
        $properties['type'] = 'float';
        $this->addOption($properties);
    }

                        /*
                         * subcommand management methods
                         */

    /**
     * Returns the parent command, if any
     *
     * @return CodeRage\Util\CommandLine
     */
    final function parent()
    {
        return $this->parent;
    }

    /**
     * Returns the list of subcommands
     *
     * @return array An array of instances of CodeRage\Util\SubCommand
     */
    final function subcommands()
    {
        return $this->subcommands;
    }

    /**
     * Returns true if a subcommand with the given name exists
     *
     * @param string $name
     * @return boolean
     */
    final function hasSubcommand($name)
    {
        return isset($this->subcommands[$name]);
    }

    /**
     * Returns the subcommand with the given name
     *
     * @param string $name
     * @return CodeRage\Util\SubCommand
     * @throws CopdeRage\Error if no such subcommand exists
     */
    final function lookupSubcommand($name)
    {
        if (!isset($this->subcommands[$name]))
            throw new
                Error([
                    'status' => 'OBJECT_DOES_NOT_EXIST',
                    'message' => "No such subcommand: $name"
                ]);
        return $this->subcommands[$name];
    }

    /**
     * Adds a command to the collection of this command line's subcommands
     *
     * @param mixed $cmd An instance of CodeRage\Util\CommandLine or an
     *   associative array to be passed to the
     *   CodeRage\Util\SubCommand constructor
     */
    final function addSubcommand($cmd)
    {
        if (is_array($cmd))
            $cmd = new CommandLine($cmd);
        Args::check($cmd, 'CodeRage\Util\CommandLine', 'sub command');
        $name = $cmd->name();
        if (isset($this->subcommands[$name]))
            throw new
                Error([
                    'status' => 'OBJECT_EXISTS',
                    'message' => "Duplicate subcommand: $name"
                ]);
        $this->subcommands[$name] = $cmd;
        $cmd->parent = $this;
    }

                        /*
                         * Methods for managing parsed values
                         */

    /**
     * Returns true if the given option has been set
     *
     * @param string $option
     */
    public final function hasValue($option)
    {
        $opt = $this->lookupOption($option);
        return $opt->hasValue();
    }

    /**
     * Returns true if the given option has a value that was explicitly
     * specified
     *
     * @return boolean
     */
    public final function hasExplicitValue($option)
    {
        $opt = $this->lookupOption($option);
        return $opt->hasExplicitValue();
    }

    /**
     * Returns the value or list of values, if any, associated with the given
     * option
     *
     * @param string $option
     * @param boolean $multiple true if a list of values, representing the
     *   values specified with multiple occurrences or the given option, should
     *   be returned; if false, only the first occurrence will be returned. Does
     *   not apply to boolean values.
     * @throws CodeRage\Error if no such option exists
     */
    public final function getValue($option, $multiple = false)
    {
        $opt = $this->lookupOption($option);
        return $opt->value($multiple);
    }

    /**
     * Sets the value of the given option
     *
     * @param string $option
     * @param string $value
     * @return mixed
     * @throws CodeRage\Error if no such option exists
     */
    public final function setValue($option, $value)
    {
        $opt = $this->lookupOption($option);
        $opt->setValue($value, true);
    }

    /**
     * Returns an associative array mapping option keys to values or lists of
     * values.
     *
     * @return array
     */
    public final function values()
    {
        $result = [];
        foreach ($this->options as $opt)
            if ($opt->hasValue())
                $result[$opt->key()] = $opt->value(true);
        return $result;
    }

    /**
     * Returns the switch option with an action specified in the most recent
     * call to parse(), if any
     *
     * @return CodeRage\Util\CommandLineOption
     */
    public final function activeSwitch()
    {
        return $this->activeSwitch;
    }

    /**
     * Returns the portion of the command line following the option list
     *
     * @return array An array of strings
     */
    public final function arguments()
    {
        return $this->arguments;
    }

    /**
     * Returns the subcommand specified in the most recent call to parse(), if
     * any
     *
     * @return CodeRage\Util\SubCommand
     */
    public final function activeSubcommand()
    {
        return $this->activeSubcommand;
    }

                        /*
                         * Methods for parsing and execution
                         */

    /**
     * Parses this command line
     *
     * @param $options array The options array; supports the following options:
     *     throwOnError - true to throw an exception if an error occurs;
     *       otherwise, prints an error and ereturns; defaults to true
     *     argv - An argument vector; if not supplied, one will be constructed
     *       from the environment
     * @throws CodeRage\Error if the command-line is invalid and throwOnError
     *   is true
     */
    public final function parse(array $options = [])
    {
        $throwOnError =
            Args::checkKey($options, 'throwOnError', 'boolean', [
                'default' => true
            ]);
        $argv =
            Args::checkKey($options, 'argv', 'list[string]', [
                'default' => $GLOBALS['argv'] ??  $_SERVER['argv'] ?? null
            ]);
        if ($argv === null)
            throw new
                Error([
                    'status' => 'MISSING_PARAMETER',
                    'message' => 'Missing argument vector'
                ]);
        try {
            $this->parseImpl($argv);
        } catch (Throwable $e) {
            if (!$throwOnError) {
                echo $e->getMessage() . PHP_EOL;
            } else {
                throw $e;
            }
        }
    }

    /**
     * Parses and executes this command line
     *
     * @param $options array The options array; supports the following options:
     *     throwOnError - true to throw an exception if an error occurs;
     *       otherwise, prints an error and returns; defaults to false
     *     argv - An argument vector; if not supplied, one will be constructed
     *       from the environment
     * @throws CodeRage\Error if the command-line is invalid and throwOnError
     *   is true
     */
    public final function execute($options = [])
    {
        $runner = $this->noEngine ?
            function($callable) { return $callable(); } :
            function($callable)
            {
                $engine = new \CodeRage\Sys\Engine;
                return $engine->run($callable, ['throwOnError' => false]);
            };
        return $runner(function() use($options) {
            try {
                $this->parse($options);
                $cmd = $this;
                while ($cmd->activeSubcommand !== null)
                    $cmd = $cmd->activeSubcommand;
                if ($cmd->activeSwitch !== null) {
                    $action = $cmd->activeSwitch->action();
                    return $action($cmd);
                } elseif ($cmd->action !== null) {
                    $exec = $cmd->action();
                    return $exec($cmd);
                } else {
                    return $cmd->doExecute();
                }
            } catch (Throwable $e) {
                if (!($options['throwOnError'] ?? false)) {
                    echo $e . PHP_EOL;
                } else {
                    throw $e;
                }
            }
        });
    }

    /**
     * Clears the underlying values, argument list, active switch, and
     * active subcommand
     */
    public final function clear()
    {
        foreach ($this->options as $opt)
            $opt->setValue($opt->default(), false);
        $this->activeSwitch = null;
        $this->arguments = [];
        $this->activeSubcommand = null;
    }

    /**
     * Returns a help message for this command line or subcommand
     *
     * @return string
     */
    public final function usage()
    {
        $this->preParse();
        $sections = [];

        // Construct sequance of command names
        $names = [];
        $cmd = $this;
        do {
            array_unshift($names, $cmd->name);
        } while ($cmd = $cmd->parent);
        $command = join(' ', $names);

        // Handle synopsis
        if (!empty($this->synopsis)) {
            $usage = "SYNOPSIS\n";
            foreach ($this->synopsis as $s)
                $usage .= "  $command $s\n";
            $sections[] = $usage;
        }

        // Handle description
        if ($this->description !== null) {
            $sections[] =
                "DESCRIPTION\n" .
                Text::wrap($this->description, self::LINE_LENGTH, '  ');
        }

        // Handle examples
        if (!empty($this->examples)) {
            $usage = "EXAMPLES\n";
            foreach ($this->examples as $e)
                $usage .= "  $command $e\n";
            $sections[] = $usage;
        }

        // Handle notes
        if ($this->notes !== null) {
            $sections[] =
                "NOTES\n" . Text::wrap($this->notes, self::LINE_LENGTH, '  ');
        }

        // Handle options
        if (!empty($this->options)) {
            $usage = "OPTIONS\n";

            // Collect list of option names with hypens and placeholders
            $names = [];
            $maxLength = 0;
            foreach ($this->options as $opt) {
                $long = $opt->longForm();
                $short = $opt->shortForm();
                $name = $short ?
                    "  -$short, --$long " :
                    ( sizeof($this->shortForms) ?
                          "      --$long " :
                          "  --$long " );
                $name .= ($ph = $opt->placeholder()) ?
                    ($opt->valueOptional() ? "[$ph] " : "$ph  ") :
                    " ";
                $names[] = $name;
                $maxLength = max($maxLength, strlen($name));
            }

            // Format options
            foreach ($this->options as $opt) {
                $desc = $opt->description() ?
                    $opt->description() :
                    'no description available';
                $name = str_pad(array_shift($names), $maxLength, ' ');
                $prefixes = [$name, str_repeat(' ', strlen($name))];
                $usage .= Text::wrap($desc, self::LINE_LENGTH, $prefixes);
            }

            $sections[] = $usage;
        }

        // Handle subcommands
        if (!empty($this->subcommands)) {
            $usage = "SUBCOMMANDS\n";

            // Collect list of subcommand names
            $names = [];
            $maxLength = 0;
            foreach ($this->subcommands as $cmd) {
                $name = '  ' . $cmd->name();
                $names[] = $name;
                $maxLength = max($maxLength, strlen($name));
            }

            // Format subcommands
            foreach ($this->subcommands as $cmd) {
                $desc = $cmd->description() ?
                    $cmd->description() :
                    'no description available';
                $name = str_pad(array_shift($names), $maxLength + 2, ' ');
                $prefixes = [$name, str_repeat(' ', strlen($name))];
                $usage .= Text::wrap($desc, self::LINE_LENGTH, $prefixes);
            }
            $usage .=
                "\nType `$command help <command>' for help on a specific command";

            $sections[] = $usage;
        }

        if ( $this instanceof CommandLine &&
             ( $this->bugEmail() !== null || $this->copyright() !== null) )
        {
            $parts = [];
            if ($this->bugEmail() !== null)
                $pages[] = $this->bugEmail();
            if ($this->copyright() !== null)
                $pages[] = $this->copyright();
            $sections[] = join("\n", $parts);
        }

        return join("\n", $sections) . "\n";
    }

                        /*
                         * Overridable methods
                         */

    /**
     * Called immediately before parsing or automatic usage generation; the
     * default implementation adds the automatically generated help and version
     * options and subcommands, as appropriate. Classes that override this
     * method must call the parent implementation
     */
    protected function doPreParse()
    {
        if ($this->version !== null) {
            if (!$this->hasOption('version'))
                $this->addOption([
                    'longForm' => 'version',
                    'shortForm' => 'v',
                    'type' => 'switch',
                    'label' => 'version',
                    'description' => 'Displays the version',
                    'action' => function($cmd) { echo $this->version; }
                ]);
            if ( count($this->subcommands) > 0 &&
                 !$this->hasSubcommand('version') )
            {
                $this->addSubcommand([
                    'name' => 'version',
                    'description' => 'Displays the version',
                    'action' => function($cmd) { echo $this->version; }
                ]);
            }
        }
        if (!$this->helpless) {
            if (!$this->hasOption('help'))
                $this->addOption([
                    'longForm' => 'help',
                    'shortForm' => 'h',
                    'type' => 'switch',
                    'label' => 'help',
                    'description' => 'Displays this help',
                    'action' =>
                        function($cmd)
                        {
                            $formatter = $cmd->formatter();
                            echo $formatter($cmd);
                        }
                ]);
            if (count($this->subcommands) > 0 && !$this->hasSubcommand('help'))
                $this->addSubcommand([
                    'name' => 'help',
                    'description' => 'Displays this help',
                    'action' =>
                        function($cmd)
                        {
                            $args = $cmd->arguments();
                            if (empty($args)) {
                                echo $this->usage();
                            } elseif ($sub = $this->lookupSubcommand($args[0])) {
                                echo $sub->usage();
                            }
                        }
                ]);
        }
    }

    /**
     * Called immediately after parsing; classes that override this method
     * must call the parent implementation
     */
    protected function doPostParse() { }

    /**
     * Performs the action associated with this command line or subcommand
     *
     * @return boolean
     */
    protected function doExecute()
    {
        echo $this->usage();
        return false;
    }

                        /*
                         * Private methods
                         */

    /**
     * Calls doPreParse() if has not yet been called
     */
    private function preParse()
    {
        if (!$this->preParsed) {
            $this->preParsed = true;
            $this->doPreParse();
        }
    }

    /**
     * Parses this comand-line
     *
     * @param array $argv The argument vector
     * @throws CodeRage\Error if the command-line is invalid
     */
    private function parseImpl($argv)
    {
        $this->preParse();
        $this->clear();

        // Construct an array mapping option keys to value lists
        $options = [];
        $z = 1;
        $n = sizeof($argv);
        while ($z < $n) {
            $arg = $argv[$z];
            if ($arg == '--') {

                // $arg is a separator between the options list and the
                // argument list
                ++$z;
                break;

            } elseif ( strlen($arg) > 3 && $arg[0] == '-' && $arg[1] == '-' &&
                       $arg[2] != '-' )
            {
                // Interpret $arg as a long option
                if (($pos = strpos($arg, '=')) !== false) {
                    $opt = $this->lookupOption(substr($arg, 2, $pos - 2));
                    $key = $opt->key();
                    if (!isset($options[$key]))
                        $options[$key] = [];
                    $options[$key][] = $pos < strlen($arg) - 1 ?
                        substr($arg, $pos + 1) :
                        '';
                } else {
                    $opt = $this->lookupOption(substr($arg, 2));
                    $key = $opt->key();
                    if (!isset($options[$key]))
                        $options[$key] = [];
                    if ($opt->type() == 'switch' || $opt->type() == 'boolean') {
                        $options[$key][] = true;
                        if ($opt->action() !== null)
                            $this->setActiveSwitch($opt);
                    } elseif ($opt->valueOptional()) {
                        if ( $z < $n - 1 &&
                             ( strlen($argv[$z + 1]) == 1 ||
                               $argv[$z + 1][0] != '-' ) )
                        {
                            $options[$key][] = $argv[++$z];
                        } else {
                             $options[$key][] = true;
                        }
                    } elseif ( $z < $n - 1 &&
                               ( strlen($argv[$z + 1]) == 1 ||
                                 $argv[$z + 1] == '' ||
                                 $argv[$z + 1][0] != '-' ) )
                    {
                        $options[$key][] = $argv[++$z];
                    } else {
                        throw new
                            Error([
                                'status' => 'INVALID_PARAMETER',
                                'message' =>
                                    "The option '$arg' requires an argument"
                            ]);
                    }
                }
            } elseif (strlen($arg) > 1 && $arg[0] == '-') {

                // $arg is a short option or combination of short options
                for ($w = 1, $m = strlen($arg); $w < $m; ++$w) {
                    $opt = $this->lookupOption($arg[$w]);
                    $key = $opt->key();
                    if (!isset($options[$key]))
                        $options[$key] = [];
                    if ($opt->type() == 'switch' || $opt->type() == 'boolean') {
                        $options[$key][] = true;
                        if ($opt->action() !== null)
                            $this->setActiveSwitch($opt);
                    } elseif ($opt->valueOptional()) {
                        if ($w < $m - 1) {
                            $options[$key][] = substr($arg, $w + 1);
                        } elseif ( $z < $n - 1 &&
                                   ( strlen($argv[$z + 1]) == 1 ||
                                     $argv[$z + 1][0] != '-' ) )
                        {
                            $options[$key][] = $argv[++$z];
                        } else {
                            $options[$key][] = true;
                        }
                        break;
                    } elseif ($w < $m - 1) {
                        $options[$key][] = substr($arg, $w + 1);
                        break;
                    } elseif ( $z < $n - 1 &&
                               ( strlen($argv[$z + 1]) <= 1 ||
                                 $argv[$z + 1][0] != '-' ) )
                    {
                        $options[$key][] = $argv[++$z];
                        break;
                    } else {
                        throw new
                            Error([
                                'status' => 'INVALID_PARAMETER',
                                'message' =>
                                    "The option '$arg' requires an argument"
                            ]);
                    }
                }
            } elseif (isset($this->subcommands[$arg])) {
                $cmd = $this->subcommands[$arg];
                if ($this->activeSwitch !== null)
                    throw new
                        Error([
                            'status' => 'INCONSISTENT_PARAMETERS',
                            'message' =>
                                "The option $this->activeSwitch cannot be " .
                                "used with subcommands"
                        ]);
                $this->activeSubcommand = $cmd;
                $cmd->parseImpl(array_slice($argv, $z));
                $argv = array();
                break;
            } else {
                break;
            }

            ++$z;
        }
        $arguments = array_slice($argv, $z);

        // Validate and normalize options
        foreach ($options as $key => $values) {
            $opt = $this->lookupOption($key);
            if (!$opt->multiple() && sizeof($values) > 1)
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "The option $opt may occur only once"
                    ]);
            for ($z = 0, $n = sizeof($values); $z < $n; ++$z) {
                $v = $values[$z];
                if (is_bool($v))
                    continue;
                switch ($opt->type()) {
                case 'int':
                    if (!is_numeric($v) || intval($v) != floatval($v))
                        throw new
                            Error([
                                'status' => 'INVALID_PARAMETER',
                                'message' =>
                                    "The value of $opt must be an integer; " .
                                    "'$v' provided"
                            ]);
                    $values[$z] = intval($v);
                    break;
                case 'float':
                    if (!is_numeric($v))
                        throw new
                            Error([
                                'status' => 'INVALID_PARAMETER',
                                'message' =>
                                    "The value of $opt must be a floating " .
                                    "point value; '$v' provided"
                            ]);
                    $values[$z] = floatval($v);
                    break;
                default:
                    break;
                }
            }
            $opt->setValue($opt->multiple() ? $values : $values[0], true);
        }

        // Check required options
        foreach ($this->options as $opt) {
            if ($opt->required() && !isset($options[$opt->key()])) {
                $name = $opt->longForm() ?
                    '--' . $opt->longForm() :
                    '-' . $opt->shortForm();
                throw new
                    Error([
                        'status' => 'MISSING_PARAMETER',
                        'message' => "The option $name is required"
                    ]);
            }
        }

        // Set arg list
        $this->arguments = $arguments;

        $this->doPostParse();
    }

    private function setActiveSwitch(CommandLineOption $switch)
    {
        if ($this->activeSwitch !== null) {
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'message' =>
                        "The options $this->activeSwitch and $switch are " .
                        "incompatible"
                ]);
        }
        $this->activeSwitch = $switch;
    }

    /**
     * The command-name, as it appears on the command-line
     *
     * @var array
     */
    private $name;

    /**
     * Description of this command
     *
     * @var string
     */
    private $description;

    /**
     * Supplementary information about this command
     *
     * @var string
     */
    private $notes;

    /**
     * The list of abstract usage examples
     *
     * @var array
     */
    private $synopsis;

    /**
     * The list of concrete usage examples
     *
     * @var array
     */
    private $examples;

    /**
     * A callback taking an instance of CodeRage\Util\Command base as an
     * argument, used to implement execute()
     *
     * @var callable
     */
    private $action;

    /**
     * true if automatic generation of the option --help and the help subcommand
     * should be suppressed
     *
     * @var boolean
     */
    private $helpless;

    /**
     * The version string, if any
     *
     * @var string
     */
    private $version;

    /**
     * The copyright notice, if any
     *
     * @var string
     */
    private $copyright;

    /**
     * The email address for reporting bugs, if any
     *
     * @var string
     */
    private $bugEmail;

    /**
     * Callback taking an instance of CodeRage\Util\CommandLine and returning
     * a string; used to implement the option --help and the help subcommand
     *
     * @var callable
     */
    private $formatter;

    /**
     * false to execute this command's action within CodeRage\Sys\Engine::run()
     *
     * @var boolean
     */
    private $noEngine;

    /**
     * The list of command-line options
     *
     * @var array
     */
    private $options = [];

    /**
     * The collection command-line options, indexed by long form
     *
     * @var array
     */
    private $longForms = [];

    /**
     * The collection command-line options, indexed by short form
     *
     * @var array
     */
    private $shortForms = [];

    /**
     * The switch option with an action specified in the most recent call to
     * parse(), if any
     *
     * @var CodeRage\Util\CommandLineOption
     */
    private $activeSwitch;

    /**
     * The parent command, if this command is a subcommand.
     *
     * @var CodeRage\Util\CommandLine
     */
    private $parent;

    /**
     * The list of subcommands, indexed by name
     *
     * @var array
     */
    private $subcommands = [];

    /**
     * The subcommand specified in the most recent call to parse(), if any
     *
     * @var CodeRage\Util\SubCommand
     */
    private $activeSubcommand;

    /**
     * The portion of the command line following the option list
     *
     * @var array
     */
    private $arguments = [];

    /**
     * true if doPreParse()
     *
     * @var unknown
     */
    private $preParsed = false;
}
