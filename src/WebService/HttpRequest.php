<?php

/**
 * Defines the class CodeRage\WebService\HttpRequest
 * 
 * File:        CodeRage/WebService/HttpRequest.php
 * Date:        Tue May 15 13:05:33 MDT 2012
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\WebService;

use Exception;
use Throwable;
use CodeRage\Error;
use CodeRage\File;
use CodeRage\Util\Args;
use CodeRage\Util\Bytes;

/**
 * @ignore
 */

/**
 * Very simple HTTP request class implemented using cURL, useful for testing
 */
class HttpRequest {

    /**
     * Regular expression mathing lines of the for HTTP/x.x XXX ...
     *
     * @var string
     */
    const MATCH_HTTP_LINE = '#^HTTP/\d(\.\d)? \d{3}\b#';

    /**
     * Regular expression used to parse headers
     *
     * @var string
     */
    const MATCH_HEADER = '/^([-a-zA-Z0-9]+):[ \t]*([^\r]+(\r\n[ \t][^\r]*)*)?$/';

    /**
     * Construcs an instance of CodeRage\WebService\HttpRequest
     *
     * @param string $uri The request URI
     * @param string $method The request method; defaults to GET
     * @param string $body The request body, if any
     * @param $headerFields A list of headers fields, represented as pairs of
     *   the form  ($name, $value) or as strings of the form "$name: $value"
     */
    public function
        __construct(
            $uri,
            $method = 'GET',
            $body = null,
            $headerFields = []
        )
    {
        $this->uri = $uri;
        $this->setMethod($method);
        foreach ($headerFields as $f) {
            list($name, $value) = is_string($f) ?
                self::parseHeaderField($f) :
                $f;
            $this->addHeaderField($name, $value);
        }
        if ($body !== null)
            $this->setBody($body);
    }

    /**
     * Returns the request URI
     *
     * @return string
     */
    public function uri()
    {
        return $this->uri;
    }

    /**
     * Sets the request URI
     *
     * @param string $uri The URI
     */
    public function setUrl($uri)
    {
        $this->uri = $uri;
    }

    /**
     * Returns the request method
     *
     * @return string
     */
    public function method()
    {
        return $this->method;
    }

    /**
     * Sets the request method
     *
     * @param string $method The method name
     */
    public function setMethod($method)
    {
        self::validateMethod($method);
        $this->method = $method;
    }

    /**
     * Returns the value of the header with the given name, if any. If there are
     * multiple headers with the same name, returns the value of the first
     * header.
     *
     * @param string
     * @return string
     */
    public function headerField($name)
    {
        $name = strtolower($name);
        return isset($this->headers[$name]) ?
            $this->headers[$name][0] :
            null;
    }

    /**
     * Returns the list of headers with the given name
     *
     * @param string
     * @return string
     */
    public function headerFieldList($name)
    {
        $name = strtolower($name);
        return isset($this->headers[$name]) ?
            $this->headers[$name] :
            null;
    }

    /**
     * Sets the specified header
     *
     * @return string
     */
    public function setHeaderField($name, $value)
    {
        $name = strtolower($name);
        return $this->headers[$name] = [$value];
    }

    /**
     * Adds the specificed header. If multple headers with the given name are
     * allowed, appends the given header to the list of headers with the given
     * name; otherwise calls setHeader().
     *
     * param string $name The header name
     * param string $value The header value
     */
    public function addHeaderField($name, $value)
    {
        $name = strtolower($name);
        if (self::multipleHeadersAllowed($name)) {
            $this->headers[$name][] = $value;
        } else {
            $this->setHeaderField($name, $value);
        }
    }

    /**
     * Returns the request body
     *
     * @return string
     */
    public function body()
    {
        return $this->body;
    }

    /**
     * Sets the request body
     *
     * @param string $body The request body
     */
    public function setBody($body)
    {
        if ($this->body !== null)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'message' => 'The body has already been set'
                ]);
        if ($this->inputFile !== null)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'message' => 'The input file has already been set'
                ]);
        $this->body = $body;
    }

    /**
     * Sets the maximum number of redirects
     *
     * @param int $maxRedirects
     */
    public function setMaxRedirects($maxRedirects)
    {
        $this->maxRedirects = $maxRedirects;
    }

    /**
     * Sets the timeout
     *
     * @param int $timeout The timeout, in seconds
     */
    public function setTimeout($timeout)
    {
        $this->timeout = $timeout;
    }

    /**
     * Sets the HTTP username and password
     *
     * @param $username The HTTP username
     * @param $password The HTTP password
     */
    public function setCredentials($username, $password)
    {
        $this->credentials = "$username:$password";
    }

    /**
     * Sets the file containing the request body
     *
     * @param string $path The file path
     */
    public function setInputFile($path)
    {
        if ($this->body !== null)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'message' => 'The body has already been set'
                ]);
        if ($this->inputFile !== null)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'message' => 'The input file has already been set'
                ]);
        File::checkReadable($path);
        $this->inputFile = $path;
    }

    /**
     * Sets the file, if any, to which the response body should be written
     *
     * @param string $path The file path; defaults to a newly created temporary
     *   file
     */
    public function setOutputFile($path = null)
    {
        if ($path == null) {
            $path = File::temp();
        } else {
            if (is_dir($path)) {
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "The file '$dir' is a directory"
                    ]);
            } elseif (is_file($path)) {
                if (!is_writable($path))
                    throw new
                        Error([
                            'status' => 'INVALID_PARAMETER',
                            'message' => "The file '$path' is not writable"
                        ]);
            } else {
                $dir = dirname($path);
                if (!file_exists($dir))
                    throw new
                        Error([
                            'status' => 'OBJECT_DOES_NOT_EXIST',
                            'message' =>
                                "The directory '$dir' does not exist"
                        ]);
                if (!is_dir($dir))
                    throw new
                        Error([
                            'status' => 'INVALID_PARAMETER',
                            'message' =>
                                "The file '$dir' is not a directory"
                        ]);
                if (!is_writable($dir))
                    throw new
                        Error([
                            'status' => 'INVALID_PARAMETER',
                            'message' =>
                                "The directory '$dir' is not writable"
                        ]);
            }
        }
        $this->outputFile = $path;
    }

    /**
     * Submits this request
     *
     * @param array $options The options array; supports the following options:
     *   throwOnError - throw an exception if the HTTP respose has a status code
     *     other than 2xx; defaults to false
     * @return CodeRage\WebService\HttpResponse
     */
    public function submit($options = [])
    {
        Args::checkKey($options, 'throwOnError', 'boolean', [
            'label' => 'throwOnErrorFlag',
            'default' => false
        ]);
        if ( $this->body === null &&
             $this->inputFile === null &&
             self::messageBodyRequired($this->method) )
        {
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'message' =>
                        "The method '$this->method' requires a message body"
                ]);
        }

        // Initialize cURL handle
        $ch = $inputFh = $outputFh = $responseHeaders = null;
        set_error_handler([$this, 'handleError']);
        try {
            $ch = curl_init();
            curl_setopt($ch, CURLOPT_URL, $this->uri);
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
            curl_setopt($ch, CURLOPT_SSLVERSION, 1);
            if ($this->method == 'POST' && $this->inputFile === null) {
                curl_setopt($ch, CURLOPT_POST, 1);
                curl_setopt($ch, CURLOPT_POSTFIELDS, $this->body);
            } elseif ($this->method !== 'GET') {
                curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $this->method);
                $size = null;
                if ($this->body !== null) {
                    $size = Bytes::strlen($this->body);
                    $inputFh = tmpfile();
                    fwrite($inputFh, $this->body);
                    fseek($inputFh, 0);
                } elseif ($this->inputFile !== null) {
                    $size = filesize($this->inputFile);
                    $inputFh = fopen($this->inputFile, 'r');
                }
                if ($inputFh) {
                    curl_setopt($ch, CURLOPT_PUT, true);
                    curl_setopt($ch, CURLOPT_INFILESIZE, $size);
                    curl_setopt($ch, CURLOPT_INFILE, $inputFh);
                }
            }
            if ($this->maxRedirects) {
                curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
                curl_setopt($ch, CURLOPT_MAXREDIRS, $this->maxRedirects);
            }
            if ($this->timeout)
                curl_setopt($ch, CURLOPT_TIMEOUT, $this->timeout);
            if ($this->credentials !== null) {
                curl_setopt($ch, CURLOPT_HTTPAUTH, CURLAUTH_ANY);
                curl_setopt($ch, CURLOPT_USERPWD, $this->credentials);
            }
            $requestHeaders = [];
            foreach ($this->headers as $name => $values) {
                switch ($name) {
                case 'referer':
                    curl_setopt($ch, CURLOPT_INFILE, $values[0]);
                    break;
                case 'user-agent':
                    curl_setopt($ch, CURLOPT_USERAGENT, $values[0]);
                    break;
                default:
                    foreach ($values as $v)
                        $requestHeaders[] = "$name: $v";
                    break;
                }
            }
            curl_setopt($ch, CURLOPT_HTTPHEADER, $requestHeaders);
            curl_setopt($ch, CURLOPT_HEADER, false);
            $responseHeaders = [];
            curl_setopt(
                $ch,
                CURLOPT_HEADERFUNCTION,
                function($ch, $header) use(&$responseHeaders)
                {
                    if (preg_match(self::MATCH_HTTP_LINE, $header)) {
                        array_splice($responseHeaders, 1);
                    } else {
                        $field = rtrim($header);
                        if ($field) {
                            $responseHeaders[] =
                                HttpRequest::parseHeaderField($field);
                        }
                    }
                    return strlen($header);
                }
            );
            if ($this->outputFile) {
                $outputFh = fopen($this->outputFile, 'w');
                curl_setopt($ch, CURLOPT_FILE, $outputFh);
            }
        } catch (Throwable $e) {
            $this->cleanup($ch, [$inputFh, $outputFh]);
            throw $e;
        }

        // Submit request
        $output = null;
        if ($outputFh) {
            $success = curl_exec($ch);
        } else {
            ob_start();
            $success = curl_exec($ch);
            $output = ob_get_contents();
            ob_end_clean();
        }
        if (!$success) {
            $details = curl_error($ch);
            $this->cleanup($ch, [$inputFh, $outputFh]);
            throw new
                Error([
                    'status' => 'HTTP_ERROR',
                    'details' => $details
                ]);
        }

        // Check status
        $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        if ( ($status < 200 || $status >= 300) &&
             $options['throwOnError'] )
        {
            $body = $outputFh ?
                file_get_contents($this->outputFile) :
                $output;
            $this->cleanup($ch, [$inputFh, $outputFh]);
            throw new
                Error([
                    'status' => 'HTTP_ERROR',
                    'details' =>
                        "Failed requesting URI '$this->uri': HTTP status " .
                        "$status (body = '$body')"
                ]);
        }

        // Process output
        $responseBody = $outputFh ?
            null :
            $output;
        $response =
            new HttpResponse(
                    $status,
                    $responseHeaders,
                    $responseBody,
                    $this->outputFile
                );

        // Clean up
        $this->cleanup($ch, [$inputFh, $outputFh]);

        return $response;
    }

    /**
     * Performs a GET request with the given request URI
     *
     * @param string $uri The request URI
     * @return CodeRage\WebService\HttpResponse
     * @throws CodeRage\Error if the HTTP status code is not 200
     */
    public static function get($uri)
    {
        $request = new HttpRequest($uri);
        $response = $request->submit();
        if ($response->status() != 200)
            throw new
                Error([
                    'status' => 'HTTP_ERROR',
                    'message' => "Failed requesting '$uri'"
                ]);
        return $response;
    }

    /**
     * Throws an instance of CodeRage\Error with status INTERNAL_ERROR
     *
     * @param int $errno The level of the error raised.
     * @param string $errstr The error message.
     * @param string $errfile The filename that the error was raised in.
     * @param int $errline The line number the error was raised at.
     */
    public function handleError($errno, $errstr, $errfile, $errline)
    {
        $message =
            \CodeRage\Util\ErrorHandler::errorCategory($errno) .
            ": {$errstr} in $errfile on line $errline";
        throw new Error(['details' => $message]);
    }

    /**
     * Cleans up the given resources and restores the error handler
     *
     * @param resource $ch A cURL handle
     * @param resource $files A list of file handles
     */
    private function cleanup($ch, $files = [])
    {
        restore_error_handler();
        if (is_resource($ch))
            @curl_close($ch);
        foreach ($files as $f)
            if (is_resource($f))
                @fclose($f);
    }

    /**
     * Throws an exception if the given method is not supported
     *
     * @param string $method The method name
     */
    private static function validateMethod($method)
    {
        static $methods =
            [
                'OPTIONS' => 1,
                'GET' => 1,
                'HEAD' => 1,
                'POST' => 1,
                'PUT' => 1,
                'DELETE' => 1,
                'TRACE' => 1,
                'CONNECT' => 1,
            ];
        if (!isset($method))
            throw new
                Error([
                    'status' => 'UNSUPPORTED_OPERATION',
                    'message' => "The method '$method' is not supported"
                ]);
    }

    /**
     * Returns true if an HTTP request with the given method requires a message
     * body
     *
     * @param string $method The method name
     * @return boolean
     */
    private static function messageBodyRequired($method)
    {
        static $methods =
            [
                'POST' => 1,
                'PUT' => 1
            ];
        return isset($methods[$method]);
    }

    /**
     * Returns true if an HTTP request may contain multiple headers field with
     * the given name
     *
     * @param string $name The header field name
     * @return boolean
     */
    private static function multipleHeadersAllowed($name)
    {
        $name = strtolower($name);
        static $names =
            [
                'authorization' => 1,
                'from' => 1,
                'host' => 1,
                'if-match' => 1,
                'if-modified-since' => 1,
                'if-none-match' => 1,
                'if-range' => 1,
                'if-unmodified-since' => 1,
                'max-forwards' => 1,
                'proxy-authorization' => 1,
                'range' => 1,
                'referer' => 1,
                'user-agent' => 1
            ];
        return !isset($names[$name]);
    }

    /**
     * Parses the given HTTP response and returns a pair ($headers, $body),
     * (where $headers is a list of ($name, $value) pairs
     *
     * @param string $response The response
     * @return array
     */
    private static function parseResponse($response)
    {
        $pos = strpos($response, "\r\n\r\n");
        if ($pos === false)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' =>
                        "Missing double CRLF in HTTP response: $response"
                ]);
        $header = substr($response, 0, $pos);
        $body = $pos + 4 < strlen($response) ?
            substr($response, $pos + 4) :
            '';
        $fields = self::parseHeaders($header);
        return [$fields, $body];
    }

    /**
     * Parses the header portion of the response, returning a list of
     * ($name, $value) pairs
     *
     * @param string $header The header
     * @return array
     */
    private static function parseHeaders($header)
    {
        $fields = [];
        $currentName = $currentValue = null;
        $parts = explode("\r\n", $header);
        array_shift($parts);
        foreach ($parts as $f) {
            if ($f[0] == ' ' || $f[0] == "\t") {
                if (!$currentName)
                    throw new
                        Error([
                            'status' => 'INVALID_PARAMETER',
                            'details' =>
                                "Invalid HTTP response header: $header"
                        ]);
                $currentValue .= $f;
            } else {
                if ($currentName)
                    $fields[] = [$currentName, $currentValue];
                list($currentName, $currentValue) = self::parseHeaderField($f);
            }
        }
        if ($currentName)
            $fields[] = [$currentName, $currentValue];
        return $fields;
    }

    /**
     * Parses the given header and returns a ($name, $value) pair
     *
     * @param string $header The header
     * @return array
     */
    public static function parseHeaderField($field)
    {
        $match = null;
        if (!preg_match(self::MATCH_HEADER, $field, $match))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' => "Invalid header: $field"
                ]);
        return [strtolower($match[1]), $match[2] ?? ''];
    }

    /**
     * The request URI
     *
     * @var string
     */
    private $uri;

    /**
     * The request methods
     *
     * @var string
     */
    private $method;

    /**
     * An associative array mapping header names to lists of values
     *
     * @var string
     */
    private $headers = [];

    /**
     * The request body
     *
     * @var string
     */
    private $body;

    /**
     * The maximum number of redirects
     *
     * @var int
     */
    private $maxRedirects;

    /**
     * The timeout, in seconds
     *
     * @var int
     */
    private $timeout;

    /**
     * The HTTP credentials, if any, for use with CURLOPT_USERPWD
     *
     * @var int
     */
    private $credentials;

    /**
     * The file, if any, containing the request body
     *
     * @var string
     */
    private $inputFile;

    /**
     * The file, if any, in which the response body should be stored
     *
     * @var string
     */
    private $outputFile;
}
