<?php

/**
 * Defines the class CodeRage\Tool\Runner
 *
 * File:        CodeRage/Tool/Runner.php
 * Date:        Thu Mar 12 04:42:29 UTC 2015
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2015 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Tool;

use Exception;
use Throwable;
use CodeRage\Access\Session;
use CodeRage\Access\User;
use CodeRage\Config;
use CodeRage\Error;
use CodeRage\Log;
use CodeRage\Sys\Engine;
use CodeRage\Sys\Config\Array_ as ArrayConfig;
use CodeRage\Sys\Config\Builtin as BuiltinConfig;
use CodeRage\Util\Args;
use CodeRage\Util\Array_;
use CodeRage\Util\Factory;
use CodeRage\Util\Json;
use CodeRage\Util\Time;
use CodeRage\WebService\HttpRequest;
use CodeRage\WebService\HttpResponse;


/**
 * Allows subclasses of CodeRage\Tool\Tool tools to be executed from the
 * command-line or by web service
 */
final class Runner {

    /**
     * @var int
     */
    private const AUTHOKEN_LIFETIME = 600;

    /**
     * @var int
     */
    private const DEFAULT_WEB_REQUEST_TIMEOUT = 86400;

    /**
     * @var string
     */
    private const LOG_TAG = 'CodeRage.Tool.Runner';

    /**
     * @var string
     */
    private const URI_PATH = '/CodeRage/Tool/run.php';

    /**
     * @var string
     */
    private const DEFAULT_HOST = '127.0.0.1';

    /**
     * @var string
     */
    private const DEFAULT_PORT = null;

    /**
     * @var string
     */
    private const DEFAULT_SSL = '0';

    /**
     * Executes a tool using the given options, returning the result or throwing
     * an exception
     *
     * @param array $options The options array; supports the following options:
     *   rootauth - A session ID for user root
     *   class - The class name of the tool to execute, expressed as
     *     dot-separated identifiers
     *   logSessionId - The log session ID (optional)
     *   timeout - The timeout, in seconds
     *   debug - An Xdebug IDE key (optional)
     *   ctor - The associative array of options to pass to the tool constructor
     *   params - The associative array of options to pass to execute()
     *   encoding - The associative array of options to pass the native data
     *     encoder
     *   config - An associative array of configuration variables used to
     *     construct a configuration to replace the project configuration during
     *     tool execution (optional)
     *   session - An associate array with keys among 'username', 'password',
     *     'authtoken', 'sessionid', and 'userid', suitable for passing to
     *     CodeRage\Access\Session::authenticate() or
     *     CodeRage\Access\Session::create() (optional)
     *   returnResult - True to return the result as a native data structure,
     *     rather than print the JSON-encoded output; defaults to true
     * @return mixed A native data structure, i.e., a scalar or a value composed
     *   from scalars using array and hash references
     */
    public static function run($options)
    {
        self::processOptions($options, false);

        // Create root session
        $config = Config::current();
        $offset = $config instanceof BuiltinConfig ?
            0 : // Session expiration is checked before config is loaded
            max(0, Time::real() - Time::get());
        $session =
            \CodeRage\Access\Session::create([
                'userid' => User::ROOT,
                'lifetime' => self::AUTHOKEN_LIFETIME + $offset
            ]);
        $options['rootauth'] = $session->sessionid();

        // Post request
        $url = self::serviceUri($options);
        $bodyOptions = $options;
        unset($bodyOptions['timeout']);
        unset($bodyOptions['debug']);
        $body = Json::encode($bodyOptions);
        if ($body === Json::ERROR)
            throw new
                Error([
                    'status' => 'INTERNAL_ERROR',
                    'details' => 'Failed encoding request: ' . Json::lastError()
                ]);
        $request = new HttpRequest($url, 'POST', $body);
        $request->setHeaderField('Content-Type', 'application/json');
        $request->setHeaderField(
            'Host',
            $config->getRequiredProperty('site_domain')
        );
        $request->setTimeout($options['timeout']);
        $response = $request->submit();
        $body = $response->body();

        // Process response
        $returnResult = isset($options['returnResult']) ?
            $options['returnResult'] :
            true;
        $output = null;
        if (!$response->success()) {
            $status = $response->status();
            throw new
                Error([
                    'status' => 'HTTP_ERROR',
                    'details' =>
                        "HTTP $status " . HttpResponse::statusText($status) .
                        " (body = '$body')"
                ]);
        } elseif ($returnResult) {
            $output = Json::decode($body);
            if ($output === Json::ERROR)
                throw new
                    Error([
                        'status' => 'INTERNAL_ERROR',
                        'details' =>
                            "Failed decoding response: " . Json::lastError() .
                            " (body = '$body')"
                    ]);
            if (!isset($output->status)) {
                throw new
                    Error([
                        'status' => 'INTERNAL_ERROR',
                        'details' =>
                            "Malformed response: missing status ('$body')"
                    ]);
            }
            if ($output->status != 'SUCCESS') {
                if (!isset($output->message))
                    throw new
                        Error([
                            'status' => 'INTERNAL_ERROR',
                            'details' =>
                                "Malformed response: missing message " .
                                "(body = '$body')"
                        ]);
                $class = $options['class'];
                throw new
                     Error([
                         'status' => $output->status,
                         'message' => $output->message,
                         'details' => isset($output->details) ?
                             $output->details :
                             null
                     ]);
            }

            // Return result
            return isset($output->result) ? $output->result : null;
        } else {
            echo $body;
        }
    }

    /**
     * Executes a tool using options encoded in standard input and writes
     * the encoded result to standard output
     */
    public static function handleRequest()
    {
        $engine = new Engine;
        $engine->run(function($engine) {
            $options = null;
            try {
                $options = self::parseInput();
                self::processOptions($options, true);
                self::execute($engine, $options);
            } catch (Throwable $e) {
                $error = Error::wrap($e);
                $errorOpts =
                    [
                        'status' => $error->status(),
                        'message' => $error->message(),
                        'pretty' => isset($options['pretty']) ?
                            $options['pretty'] :
                            false
                    ];
                if ($error->details() !== $error->message())
                    $errorOpts['details'] = $error->details();
                self::outputResponse($errorOpts);
            }
        }, ['throwOnError' => false]);
    }

    /**
     * Returns a hash constructed by parsing the request data as JSON.
     */
    private static function parseInput()
    {
        // Verify that HTTP method is POST
        if ($_SERVER['REQUEST_METHOD'] != 'POST')
            throw new
                Error([
                    'status' => 'BAD_REQUEST',
                    'details' =>
                        "Expected POST; found {$_SERVER['REQUEST_METHOD']}"
                ]);

        // Read standard input
        $body = stream_get_contents(fopen('php://input', 'r'));
        self::getLog()->logMessage("Request = $body");

        // Parse JSON
        $options = Json::decode($body, ['objectsAsArrays' => true]);
        if ($options === Json::ERROR)
            throw new
                Error([
                    'status' => 'INTERNAL_ERROR',
                    'details' =>
                        "Failed decoding request: " . Json::lastError() .
                        " (body = '$body')"
                ]);
        if (!is_array($options) || Array_::isIndexed($options))
            throw new
                Error([
                    'status' => 'BAD_REQUEST',
                    'details' =>
                        "Marformed request: expected JSON object; found " .
                        "'$body'"
                ]);
        return $options;
    }

    /**
     * Constructs and executes an instance of CodeRage\Tool\Tool
     *
     * @param CodeRage\Sys\Engine $engine
     * @param array $options The options array; accepts the same options as
     *   run()
     */
    private static function execute(Engine $engine, array $options)
    {
        $session = Session::authenticate(['sessionid' => $options['rootauth']]);
        if ($session->user()->id() != User::ROOT)
            throw new
                Error([
                    'status' => 'UNAUTHORIZED',
                    'message' => 'Only root is permitted to run tools'
                ]);
        if (isset($options['logSessionId']))
            Log::current()->setSessionId($options['logSessionId']);
        self::getLog()->logMessage("Constructing tool");
        if (isset($options['config'])) {
            $config = new ArrayConfig($options['config']);
            Config::setCurrent($config);
            $offset = $config->getProperty('coderage.util.time.offset', 0);
            Time::set(Time::real() + $offset);
        }
        if (isset($options['session'])) {
            $session = isset($options['session']['userid']) ?
                Session::create($options['session']) :
                Session::authenticate($options['session']);
            Session::setCurrent($session);
        }
        $options['ctor']['engine'] = $engine;
        $tool =
            Factory::create([
                'class' => $options['class'],
                'params' => $options['ctor']
            ]);
        self::getLog()->logMessage("Executing tool");
        $result = $tool->execute($options['params']);
        $native = new \CodeRage\Util\NativeDataEncoder($options['encoding']);
        $resultOpts =
            [
                'status' => 'SUCCESS',
                'pretty' => $options['pretty']
            ];
        if ($result !== null)
            $resultOpts['result'] = $native->encode($result);
        self::outputResponse($resultOpts);
    }

    /**
     * Outputs a JSON-encoded response and exits.
     *
     * @param array $options The options array; supports the following options:
     *   status - The status code
     *   message - The error message (optional)
     *   details - The detailed error message (optional)
     *   result - The tool output (optional)
     *   encodedResult - The JSON-encoded tool output (optional)
     *   pretty - true to output formatted JSON
     *
     * @returns int The exit status
     */
    private static function outputResponse($options)
    {
        $pretty = $options['pretty'];
        unset($options['pretty']);
        try {
            header("Content-Type: application/json; charset=utf-8");
            $json = Json::encode($options, ['pretty' => $pretty]);
            if ($json === Json::ERROR)
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' =>
                            'Failed encoding response: ' . Json::lastError()
                    ]);
            echo $json;
            self::getLog()->logMessage("Response = $json");
        } catch (Throwable $e) {

            // The only thing above that can fail is encoding
            $status = 'INTERNAL_ERROR';
            self::outputResponse([
                'status' => $status,
                'message' => Error::translateStatus($status),
                'details' => "JSON encoding error: $e",
                'pretty' => $pretty
            ]);
        }
    }

    /**
     * Validates and processes options used to submit a web service request
     *
     * @param array $options The options array; supports the following options:
     *   rootauth - An authorization token for user root
     *   class - The class name of the tool to execute, expressed as
     *     dot-separated identifiers
     *   logSessionId - The log session ID (optional)
     *   timeout - The timeout, in seconds
     *   debug - An Xdebug IDE key (optional)
     *   ctor - The associative array of options to pass to the tool constructor
     *   params - The associative array of options to pass to execute()
     *   encoding - The associative array of options to pass the native data
     *     encoder
     *   config - An associative array of configuration variables used to
     *     construct a configuration to replace the project configuration during
     *     tool execution (optional)
     *   session - An associate array with keys among 'username', 'password',
     *     'authtoken', and 'sessionid', suitable for passing to
     *     CodeRage\Access\Session::authenticate() or
     *     CodeRage\Access\Session::create() (optional)
     *  @param boolean $webRequest true if the current tool execution is via
     *    web service
     */
    private static function processOptions(array &$options, $webRequest)
    {
        Args::checkKey($options, 'rootauth', 'string', [
            'label' => 'root session ID',
            'required' => $webRequest
        ]);
        Args::checkKey($options, 'class', 'string', [
            'required' => true
        ]);
        Args::checkKey($options, 'logSessionId', 'string', [
            'label' => 'log session ID'
        ]);
        Args::checkBooleanKey($options, 'pretty', [
            'label' => 'pretty flag',
            'default' => false
        ]);
        foreach (['ctor', 'params', 'encoding'] as $n) {
            self::processAssociativeOption($options, $n, [
                'default' => []
            ]);
            if (!$webRequest)
                settype($options[$n], 'object');
        }
        self::processAssociativeOption($options, 'config');
        if ( !isset($options['config']) &&
             !$webRequest &&
             !Config::current() instanceof BuiltinConfig)
        {
            $current = Config::current();
            $config = [];
            foreach ($current->propertyNames() as $name) {
                $config[$name] = $current->getProperty($name);
            }
            $options['config'] = $config;
            if (!$webRequest)
                settype($options['config'], 'object');
        }
        self::processAssociativeOption($options, 'session');
        if (isset($options['session'])) {
            $session = $options['session'];
            Args::uniqueKey($session, ['username', 'authtoken', 'sessionid', 'userid']);
            Args::checkKey($session, 'username', 'string');
            Args::checkKey($session, 'password', 'string');
            Args::checkKey($session, 'authtoken', 'string');
            Args::checkKey($session, 'sessionid', 'string', [
                'label' => 'session ID'
            ]);
            Args::checkKey($session, 'userid', 'string', [
                'label' => 'user ID'
            ]);
            if (isset($session['username']) != isset($session['password']))
                throw new
                    Error([
                        'status' => 'INCONSISTENT_PARAMETERS',
                        'details' =>
                            "The session options 'username' and 'password' " .
                            "must be specified together"
                    ]);
            //if ($webRequest)
            //    settype($options['session'], 'object');
        }
        if (!$webRequest) {
            Args::checkKey($options, 'timeout', 'int', [
                'default' => self::DEFAULT_WEB_REQUEST_TIMEOUT
            ]);
            if ($options['timeout'] < 0)
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' =>
                            "Invalid timeout: {$options['timeout']}"
                    ]);
            Args::checkKey($options, 'debug', 'string', [
                'label' => 'debug info',
                'default' => null
            ]);
        } else {
            foreach (['timeout', 'debug'] as $n)
                if (isset($options[$n]))
                    throw new
                        Error([
                            'status' => 'INVALID_PARAMETER',
                            'details' => "Unsupported option: $n"
                        ]);
        }
    }

    /**
     * Returns true if the current process is a web service request
     *
     * @return boolean
     */
    private static function isWebRequest()
    {
        return PHP_SAPI != 'cli';
    }

    /**
     * Returns an instance of CodeRage\Log, constructing one if necessary.
     */
    private static function getLog()
    {
        static $log;
        if (!$log) {
            $log = new Log;
            $log->setTag(self::LOG_TAG);
            $log->registerProvider(
                new \CodeRage\Log\Provider\Db,
                Log::INFO
            );
        }
        return $log;
    }

    /**
     * Returns the entry point of the tool runner web service
     *
     * @param array $options The options array; supports the following options:$this
     *     host - The tool runner host (optional)
     *     port - The tool runner port (optional)
     *     ssl - true to use SSL (optional)
     *     debug - The DPGP IDE key (optional)
     * @return string The URL
     */
    private static function serviceUri(array $options = [])
    {
        $config = Config::current();
        $host =
            Args::checkKey($options, 'host', 'string', [
                'default' =>
                    $config->getProperty(
                        "coderage.tool.runner.host",
                        self::DEFAULT_HOST
                    )
            ]);
        $port =
            Args::checkIntKey($options, 'port', [
                'default' =>
                    $config->getProperty(
                        "coderage.tool.runner.port",
                        self::DEFAULT_PORT
                    )
            ]);
        $ssl =
            Args::checkBooleanKey($options, 'ssl', [
                'default' =>
                    $config->getProperty(
                        "coderage.tool.runner.ssl",
                        self::DEFAULT_SSL
                    )
            ]);
        $debug =
            Args::checkKey($options, 'debug', 'string', [
                'label' => 'IDE key'
            ]);
        $uri =
            ($ssl ? 'https' : 'http') . '://' . $host .
            ($port !== null ? ':' . $port : '') . self::URI_PATH .
            ($debug !== null ? "?XDEBUG_SESSION_START=$debug" : '');
        return $uri;
    }

    /**
     * Verifies that if the named option exists in the given options array, it
     * is an ampty array or an associative array
     *
     * @param array $options A collection of named values
     * @param string $name The name of the value to be validated
     * @param array $params An associative array with keys among
     *     label - Descriptive text for use in an error message; defaults
     *       to $name
     *     required - true to cause an exception to be thrown if the value is not
     *       present or is null; defaults to false
     *     default - The default value, if any
     * @return mixed The value of the option, if any
     * @throws CodeRage\Error
     */
    private static function processAssociativeOption(
        array &$options, $name, $params = [])
    {
        $result = Args::checkKey($options, $name, 'array', $params);
        if (!empty($result) && Array_::isIndexed($result)) {
            $label = isset($params['label']) ?
                $params['label'] :
                $name;
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid $label: expected associative array; found " .
                        "indexed array"
                ]);
        }
        return $result;
    }
}
