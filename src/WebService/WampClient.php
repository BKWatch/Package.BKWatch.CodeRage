<?php

/**
 * Defines the class CodeRage\WebService\WampClient
 *
 * File:        CodeRage/WebService/WampClient.php
 * Date:        Fri Oct  9 01:24:02 UTC 2020
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\WebService;

use CodeRage\Config;
use CodeRage\Error;
use CodeRage\Util\Args;
use CodeRage\Util\Array_;
use CodeRage\Util\ExponentialBackoff;
use CodeRage\Util\Shared;

/**
 * Client for accessing a WAMP service
 */
final class WampClient {

    /**
     * Constructs an instance of CodeRage\WebService\WampClient
     *
     * @param array $options The options array; supports the following options:
     *     service - The service name, used as a configuration variable prefix
     *       to determine the host, port, and realm of the service (optional)
     *     host - The host name (optional)
     *     port - The port (optional)
     *     realm - The realm (optional)
     *     wsopts - An associative array of WebSoket options; supports the
     *       following keys:
     *         headers - An associative array of headers
     *         timeout - The socket read/write timeout
     *         fragmentSize - The fragment size
     *   Exactly one of "service" or "realm" is required; if "realm" is
     *   supplied, "host"" and "post" must also be supplied
     */
    public function __construct(array $options)
    {
        $this->processOptions($options);
        [$realm, $host, $port, $wsopts] =
            Array_::values($options, ['realm', 'host', 'port', 'wsopts']);
        if (filter_var($host, FILTER_VALIDATE_DOMAIN, FILTER_FLAG_HOSTNAME) === false)
            throw new
                Error([
                    'status' => 'CONFIGURATION_ERROR',
                    'details' => "Invalid host: $host"
                ]);
        if (!ctype_digit($port))
            throw new
                Error([
                    'status' => 'CONFIGURATION_ERROR',
                    'details' => "Invalid port: $port"
                ]);
        $port = (int) $port;
        $uri = "ws://$host:$port/ws";
        $this->uri = $uri;
        $this->realm = $realm;
        $this->wsopts = $wsopts;
    }

    /**
     * Calls the specified WAMP procedure with the given input
     *
     * @param string $uri The procedure URI
     * @param array $input The procedure input
     * @return mixed
     * @throws CodeRage\Error
     */
    public function call(string $uri, array $input)
    {
        if ($this->client === null)
            $this->client = $this->createClient();
        $output = null;
        try {
            $output = $this->callImpl($uri, $input);
        } catch (\BKWTools\WampSyncClient\Exception $e) {
            throw new
                Error([
                    'status' => 'THIRD_PARTY_SERVICE_ERROR',
                    'inner' => $e
                ]);
        } catch (\BKWTools\WebSocket\Exception $e) {
            throw new
                Error([
                    'status' => 'THIRD_PARTY_SERVICE_ERROR',
                    'inner' => $e
                ]);
        }
        Args::check(
            $output,
            'BKWTools\WampSyncClient\CallResult',
            'WAMP service output'
        );
        if (count($output->arguments) != 1)
            throw new
                Error([
                    'status' => 'INTERNAL_ERROR',
                    'details' =>
                        'Invalid WAMP service output: expected list of ' .
                        'length 1; found list of length ' .
                        count($output->arguments)
                ]);
        $result = $output->arguments[0];
        Args::check($result, 'map', 'WAMP service result');
        $status =
            Args::checkKey($result, 'status', 'string', [
                'label' => 'WAMP service status',
                'required' => true
            ]);
        if ($status == 'SUCCESS') {
            return $result['result'] ?? null;
        } else {
            $message =
                Args::checkKey($result, 'message', 'string', [
                    'label' => 'WAMP service error message',
                    'required' => true
                ]);
            throw new
                Error([
                    'status' => 'THIRD_PARTY_SERVICE_ERROR',
                    'details' => $message
                ]);
        }
    }

    /**
     * Helper method for call()
     *
     * @param string $uri The procedure URI
     * @param array $input The procedure input
     * @return mixed
     * @throws CodeRage\Error
     */
    private function callImpl(string $uri, array $input)
    {
        $error = null;
        try {
            return $this->client->get()->call($uri, $input);
        } catch (\BKWTools\WampSyncClient\Exception $e) {
            $error = $e;
            // Fall through
        } catch (\BKWTools\WebSocket\Exception $e) {
            $error = $e;
            // Fall through
        }
        \CodeRage\Log::current()->logMessage(
            "Caught instance of " . get_class($error) . " with message " .
            $error->getMessage() . "; attempting to reconnect to WAMP server"
        );

        // WAMP router likely terminated session; attempt to reconnect
        $backoff = new ExponentialBackoff;
        $backoff->execute(
            function()
            {
                $this->client->reset();
            },
            function($e)
            {
                return $e instanceof \BKWTools\WampSyncClient\Exception ||
                       $e instanceof \BKWTools\WebSocket\Exception;
            },
            "Reconnecting to WAMP router"
        );

        // Try again
        return $this->client->get()->call($uri, $input);
    }

    /**
     * Validates and processes the given options array
     *
     * @param array $options The options array passed to the constructor
     */
    private function processOptions(array &$options) : void
    {
        Args::checkKey($options, 'service', 'string');
        Args::checkKey($options, 'realm', 'string');
        $opt = Args::uniqueKey($options, ['service', 'realm']);
        if ($opt == 'service') {
            if (isset($options['host']) || isset($options['port']))
                throw new
                    Error([
                        'status' => 'INCONSISTENT_PARAMETERS',
                        'details' =>
                            "The option 'service' may not be combined with " .
                            "the options 'host' or 'port'"
                    ]);
            $service = $options['service'];
            $config = Config::current();
            foreach (['realm', 'host', 'port'] as $n)
                $options[$n] = $config->getRequiredProperty("$service.$n");
        } else {
            foreach (['host', 'port'] as $n)
                Args::checkKey($options, 'host', 'string', ['required' => true]);
        }
        Args::checkKey($options, 'wsopts', 'map', ['default' => []]);
    }

    /**
     * Returns the value of the named configuration property
     *
     * @param string $name
     * @return string
     */
    private function loadConfiguration($name) : string
    {
        $config = \CodeRage\Config::current();
        ['var' => $var, 'default' => $default] = self::CONFIG_VARS[$name];
        return $config->getProperty($var, $default);
    }

    /**
     * Returns a newly constructed WAMP client
     *
     * @return BKWTools\WampSyncClient\Client
     */
    private function createClient() : Shared
    {
        $params = [$this->uri, $this->realm, null, null, $this->wsopts];
        return new Shared('BKWTools\WampSyncClient\Client', $params);
    }

    /**
     * @var string
     */
    private $uri;

    /**
     * @var string
     */
    private $realm;

    /**
     * Wrapper for an instance of BKWTools\WampSyncClient\Client
     *
     * @var CodeRage\Util\Shared
     */
    private $client;

    /**
     * @var array
     */
    private $wsopts;
}
