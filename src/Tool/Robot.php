<?php

/**
 * Defines the trait CodeRage\Tool\Robot
 *
 * File:        CodeRage/Tool/Robot.php
 * Date:        Sun Jan 14 20:43:12 UTC 2018
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Tool;

use DOMDocument;
use DOMXPath;
use Exception;
use InvalidArgumentException;
use GuzzleHttp\Psr7\Uri;
use Symfony\Component\DomCrawler\Crawler;
use Symfony\Component\DomCrawler\Field\FileFormField;
use Symfony\Component\DomCrawler\Form;
use Throwable;
use CodeRage\Error;
use CodeRage\Log;
use CodeRage\Text;
use CodeRage\Text\Regex;
use CodeRage\Tool\RobotConstants as Constants;
use CodeRage\Tool\Robot\CaptchaSolver;
use CodeRage\Tool\Robot\ContentRecorder;
use CodeRage\Tool\Robot\FileUploadFieldSetter;
use CodeRage\Tool\Robot\RequestLogger;
use CodeRage\Util\Args;
use CodeRage\Util\Array_;

/**
 * Trait to augment CodeRage\Tool\Tool with web-crawling ability
 */
trait Robot {

    /**
     * Initializes an instance of a class that uses CodeRage\Tool\Robot
     *
     * @param array $options The options array; supports all options supported
     *   by CodeRage\Tool\Tool, plus the following options
     *     userAgent - The value of the "User-Agent" header (optional)
     *     accept - The value of the "Accept" header (optional)
     *     acceptLanguage - The value of the "Accept-Language" header (optional)
     *     timeout - The request timeout, in seconds (optional)
     *     verifyCertificate - true to enable SSL certificate verification;
     *       defaults to true
     *     proxy - The proxy server settings
     *     fetchAttempts - The number of times to attempt a request before
     *       aborting (optional)
     *     fetchSleep - The number of microseconds to wait after an initial
     *       failed request before a second attempt (optional)
     *     fetchMultiplier - The multipler for use in exponential backoff when a
     *       request fails; used in conjunction with the options "fetchAttempts"
     *       and "fetchSleep" (optional)
     */
    protected function robotInitialize(array $options = [])
    {
        // Validate and process options
        $userAgent =
            Args::checkKey($options, 'userAgent', 'string', [
                'label' => '"User-Agent" header',
                'default' => Constants::DEFAULT_USER_AGENT
            ]);
        $accept =
            Args::checkKey($options, 'accept', 'string', [
                'label' =>  '"Accept" header',
                'default' => Constants::DEFAULT_ACCEPT
            ]);
        $acceptLanguage =
            Args::checkKey($options, 'acceptLanguage', 'string', [
                'label' =>  '"Accept-Language" header',
                'default' => Constants::DEFAULT_ACCEPT_LANGUAGE
            ]);
        $timeout =
            Args::checkIntKey($options, 'timeout', [
                'default' => Constants::DEFAULT_TIMEOUT
            ]);
        $verifyCertificate =
            Args::checkBooleanKey($options, 'verifyCertificate', [
                'default' => Constants::DEFAULT_VERIFY_CERTIFICATE
            ]);
        $fetchAttempts =
            Args::checkIntKey($options, 'fetchAttempts', [
                'default' => Constants::DEFAULT_FETCH_ATTEMPTS
            ]);
        $fetchSleep =
            Args::checkIntKey($options, 'fetchSleep', [
                'default' => Constants::DEFAULT_FETCH_SLEEP
            ]);
        $fetchMultiplier =
            Args::checkNumericKey($options, 'fetchMultiplier', [
                'default' => Constants::DEFAULT_FETCH_MULTIPLIER
            ]);

        // Initialize instance
        $this->requestOptions =
           ['curl' => [CURLOPT_SSLVERSION => CURL_SSLVERSION_TLSv1]];
        $this->setTimeout($timeout);
        $this->setVerifyCertificate($verifyCertificate);
        $this->setHeader('User-Agent', $userAgent);
        $this->setHeader('Accept', $accept);
        $this->setHeader('Accept-Language', $acceptLanguage);
        if (isset($options['proxy'])) {
            $this->setProxy($options['proxy']);
        }
        $this->setFetchAttempts($fetchAttempts);
        $this->setFetchSleep($fetchSleep);
        $this->setFetchMultiplier($fetchMultiplier);
        $this->client = new \CodeRage\Tool\Robot\BrowserKitClient;
        $this->client->setRequestOptions($this->requestOptions);
    }

            /*
             * Exponential backoff management
             */

    /**
     * Returns the number of times to attempt a request before aborting
     *
     * @return int
     */
    public final function fetchAttempts()
    {
        return $this->fetchAttempts;
    }

    /**
     * Sets the number of times to attempt a request before aborting
     *
     * @param int $attempts
     */
    public final function setFetchAttempts($attempts)
    {
        Args::check($attempts, 'int', 'fetch attempts');
        if ($attempts <= 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid fetch attempts; expected positive " .
                        "integer; found $attempts"
                ]);
        $this->fetchAttempts = $attempts;
    }

    /**
     * Returns the number of microseconds to wait after an initial failed
     * request before a second attempt
     *
     * @return int
     */
    public final function fetchSleep()
    {
        return $this->fetchSleep;
    }

    /**
     * Sets the number of microseconds to wait after an initial failed
     * request before a second attempt
     *
     * @param int $sleep
     */
    public final function setFetchSleep($sleep)
    {
        Args::check($sleep, 'int', 'fetch sleep');
        if ($sleep <= 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid fetch sleep; expected positive integer; " .
                        "found $sleep"
                ]);
        $this->fetchSleep = $sleep;
    }


    /**
     * Returns the multipler for use in exponential backoff when a request
     * fails; used in conjunction with $fetchAttempts $fetchSleep
     *
     * @return float
     */
    public final function fetchMultiplier()
    {
        return $this->fetchMultiplier;
    }

    /**
     * Sets the multipler for use in exponential backoff when a request
     * fails; used in conjunction with $fetchAttempts $fetchSleep
     *
     * @param float $multiplier
     */
    public final function setFetchMultiplier($multiplier)
    {
        Args::check($multiplier, 'float', 'fetch multiplier');
        if ($multiplier <= 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid fetch multiplier; expected positive value; " .
                        "found $multiplier"
                ]);
        $this->fetchMultiplier = $multiplier;
    }

            /*
             * Request option management
             */

    /**
     * Returns the request timeout, in seconds
     *
     * @return int
     */
    public final function timeout()
    {
        return $this->getRequestOption('timeout');
    }

    /**
     * Sets the request timeout, in seconds
     *
     * @param int $timeout
     */
    public final function setTimeout($timeout)
    {
        Args::check($timeout, 'int', 'timeout');
        if ($timeout <= 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "Invalid timeout; expected positive integer; found " .
                        $timeout
                ]);
        $this->setRequestOption('timeout', $timeout);
    }

    /**
     * Returns a boolean indicating whether SSL certificates are to be verified
     *
     * @return boolean
     */
    public final function verifyCertificate()
    {
        return $this->requestOptions['curl'][CURLOPT_SSL_VERIFYPEER];
    }

    /**
     * Specifies whether SSL certificates are to be verified
     *
     * @param boolean $verify true if SSL certificates are to be verified
     */
    public final function setVerifyCertificate($verify)
    {
        Args::check($verify, 'boolean', 'verify certificate flag');
        $this->requestOptions['curl'][CURLOPT_SSL_VERIFYPEER] = $verify;
    }

    /**
     * Returns the value of the named header field, if any
     *
     * @param string $name The heaeder field name
     * @return string
     */
    public final function getHeader($name)
    {
        Args::check($name, 'string', 'header field name');
        $name = strtolower($name);
        return isset($this->requestOptions['headers'][$name]) ?
            $this->requestOptions['headers'][$name] :
            null;
    }

    /**
     * Sets the value of the named header field
     *
     * @param string $name The header field name
     * @param string $value The header field value
     */
    public final function setHeader($name, $value)
    {
        Args::check($name, 'string', 'header field name');
        Args::check($value, 'string', 'header field value');
        if (!isset($this->requestOptions['headers']))
            $this->requestOptions['headers'] = [];
        $this->requestOptions['headers'][strtolower($name)] = $value;
    }

    /**
     * Clears the value of the named header field
     *
     * @param string $name The header field name
     */
    public final function clearHeader($name)
    {
        Args::check($name, 'string', 'header field name');
        unset($this->requestOptions['headers'][strtolower($name)]);
    }

    /**
     * Returns the proxy server URI, if any
     *
     * @return string
     */
    public final function proxy()
    {
        return $this->requestOptions['proxy'] ?? null;
    }

    /**
     * Sets the proxy server to use for subsequent requests
     *
     * @param mixed $proxy The proxy server URI
     */
    public final function setProxy(string $proxy)
    {
        if ($proxy !== '') {  // Allow proxy to be set by the configuration
            $flags = FILTER_FLAG_SCHEME_REQUIRED | FILTER_FLAG_HOST_REQUIRED;
            if (!filter_var($proxy, FILTER_VALIDATE_URL, $flags)) {
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' => 'Invalid proxy URI: ' . $proxy
                    ]);
            }
            $this->requestOptions['proxy'] = $proxy;
        }
    }

    /**
     * Clears the proxy server settings
     */
    public final function clearProxy()
    {
        unset($this->requestOptions['proxy']);
    }

            /*
             * Request logger management
             */

    /**
     * Returns the list of registered request loggers
     *
     * @return array A list of instances of CodeRage\Tool\Robot\RequestLogger
     */
    public final function requestLoggers()
    {
        return $this->requestLoggers;
    }

    /**
     * Adds a request logger to the list of registered request loggers
     *
     * @param CodeRage\Tool\Robot\RequestLogger $requestLogger
     */
    public final function registerRequestLogger(RequestLogger $requestLogger)
    {
        $this->requestLoggers[] = $requestLogger;
    }

    /**
     * Removes the given request logger from the list of registered request
     * loggers
     *
     * @param CodeRage\Tool\Robot\RequestLogger $requestLogger
     */
    public final function unregisterRequestLogger(RequestLogger $requestLogger)
    {
        foreach ($this->requestLoggers as $i => $logger) {
            if ($logger === $requestLogger) {
                array_splice($this->requestLoggers, $i, 1);
                break;
            }
        }
    }

            /*
             * Content recorder management
             */

    /**
     * Returns the registered instances of CodeRage\Tool\Robot\ContentRecorder
     *
     * @return CodeRage\Tool\Robot\ContentRecorder
     */
    public final function contentRecorder()
    {
        return $this->contentRecorder;
    }

    /**
     * Sets the registered instances of CodeRage\Tool\Robot\ContentRecorder
     *
     * @param CodeRage\Tool\Robot\ContentRecorder $contentRecorder The new
     *   content recorder
     */
    public final function setContentRecorder(ContentRecorder $contentRecorder)
    {
        $this->contentRecorder = $contentRecorder;
    }

            /*
             * CAPTCHA solver management
             */

    /**
     * Returns the list of registered CAPTCHA solvers
     *
     * @return array A list of instances of CodeRage\Tool\Robot\CaptchaSolver
     */
    public final function captchaSolvers()
    {
        return $this->captchaSolvers;
    }

    /**
     * Adds a CAPTCHA solver to the list of registered CAPTCHA solvers
     *
     * @param CodeRage\Tool\Robot\CaptchaSolver $captchaSolver
     */
    public final function registerCaptchaSolver(CaptchaSolver $captchaSolver)
    {
        $this->captchaSolvers[] = $captchaSolver;
    }

    /**
     * Removes the given CAPTCHA solver from the list of registered CAPTCHA
     * solvers
     *
     * @param CodeRage\Tool\Robot\CaptchaSolver $captchaSolver
     */
    public final function unregisterCaptchaSolver(CaptchaSolver $captchaSolver)
    {
        foreach ($this->captchaSolvers as $i => $solver) {
            if ($solver === $captchaSolver) {
                array_splice($this->captchaSolvers, $i, 1);
                break;
            }
        }
    }

            /*
             * HTTP request access
             */

    /**
     * Returns the current HTTP response
     *
     * @return Psr\Http\Message\RequestInterface
     */
    public final function request()
    {
        return $this->client->getPsrRequest();
    }

            /*
             * HTTP response access
             */

    /**
     * Returns the current HTTP response
     *
     * @return Psr\Http\Message\ResponseInterface
     */
    public final function response()
    {
        return $this->client->getPsrResponse();
    }

    /**
     * Returns the body of the current HTTP response, unless the content
     * type is not text/html and an output file was specified
     *
     * @return string
     * @throws CodeRage\Error if there is no current response
     */
    public final function content()
    {
        $response = $this->client->getResponse();
        if ($response === null)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'details' => 'No current HTTP response'
                ]);
        return $response->getContent();
    }

    /**
     * Returns a DOM crawler for accessing the current response
     *
     * @return Symfony\Component\DomCrawler\Crawler
     * @throw CodeRage\Error if the page stack is empty
     */
    public final function crawler()
    {
        $crawler = $this->client->getCrawler();
        if ($crawler === null)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'details' => 'The page stack is empty'
                ]);
        return $crawler;
    }

    /**
     * Returns the result of calling CodeRage\Text\Regex::hasMatch() with the
     * current content as the $subject argument
     *
     * @param array ...$args See CodeRage\Text\Regex::hasMatch()
     * @return boolean
     */
    public final function hasMatch(...$args)
    {
        array_splice($args, 1, 0, [$this->content()]);
        return Regex::hasMatch(...$args);
    }

    /**
     * Returns the result of calling CodeRage\Text\Regex::getMatch() with the
     * current content as the $subject argument
     *
     * @param array ...$args See CodeRage\Text\Regex::getMatch()
     * @return mixed See CodeRage\Text\Regex::getMatch()
     */
    public final function getMatch(...$args)
    {
        array_splice($args, 1, 0, [$this->content()]);
        return Regex::getMatch(...$args);
    }

    /**
     * Returns the result of calling CodeRage\Text\Regex::getAllMatches() with
     * the current content as the $subject argument
     *
     * @param array ...$args See CodeRage\Text\Regex::getAllMatches()
     * @return array
     */
    public final function getAllMatches(...$args)
    {
        array_splice($args, 1, 0, [$this->content()]);
        return Regex::getAllMatches(...$args);
    }

    /**
     * Returns the value of the Content-Type header of the current HTTP
     * response
     *
     * @return string
     * @throws CodeRage\Error if there is no current response
     */
    public final function contentType()
    {
        $response = $this->client->getResponse();
        if ($response === null)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'details' => 'No current HTTP response'
                ]);
        return $response->getHeader('Content-Type');
    }

    /**
     * Returns the true of the content type of the current response is equal to
     * the given string after any parameters are removed
     *
     * @string $type A MIME media type without any parameters
     * @return bool
     */
    public final function hasContentType($type)
    {
        return $type ==
               preg_replace('/(.*?)($|\s*;.*)/', '$1', $this->contentType());
    }

    /**
     * Throws an exception with status UNEXPECTED_CONTENT if the the content
     * type of the current response is not equal to the given string after any
     * parameters are removed
     *
     * @string $type A MIME media type without any parameters
     * @throws CodeRage\Error
     */
    public final function assertContentType($type)
    {
        if (!$this->hasContentType($type))
            $this->wrongPage([
                'message' =>
                    "Expected content type '$type'; found '" .
                    $this->contentType() . "'",
                'status' => 'UNEXPECTED_BEHAVIOR'
            ]);
    }

    /**
     * Strips tags, replaces entity references and normalizes whitespace
     *
     * @param sting $html The text to process
     * @param array $options Supports the following options:
     *     preserveLinebreaks - true if HTML line breaks should be replaced with
     *     newline characters; defaults to false
     * @return The processed text
     */
    public final function cleanHtml($html, array $options = [])
    {
        return Text::htmlToText($html, $options);
    }

    /**
     * Returns a list of associative arrays with keys "name", "value",
     * "expires", "path", "domain", "secure", and "httpOnly"
     *
     * @return array
     */
    public final function cookies()
    {
        $cookies = [];
        foreach ($this->client->getCookieJar()->all() as $cookie)
            $cookies[] =
                [
                    'name' => $cookie->getName(),
                    'value' => $cookie->getValue(),
                    'expires' => $cookie->getExpiresTime(),
                    'path' => $cookie->getPath(),
                    'domain' => $cookie->getDomain(),
                    'secure' => $cookie->isSecure(),
                    'httpOnly' => $cookie->isHttpOnly(),
                ];
        return $cookies;
    }

    /**
     * Sets a cookie
     *
     * @param array $options - An associative array with keys among
     *     name - The name
     *     value - The value
     *     expires - The expiration date (optional)
     *     path - The path
     *     domain - The domain (optional)
     *     secure - The "secure" flag
     *     httpOnly - The "httponly" flag (optional)
     *     encoded - The "httponly" flag; defaults to false
     */
    public final function setCookie($options)
    {
        Args::checkKey($options, 'name', 'string', [
            'required' => true
        ]);
        Args::checkKey($options, 'value', 'string', [
            'required' => true
        ]);
        Args::checkIntKey($options, 'expires', [
            'label' => 'expiration date',
            'default' => strtotime('+1 day')
        ]);
        Args::checkKey($options, 'path', 'string', [
            'required' => true
        ]);
        Args::checkKey($options, 'domain', 'string', [
            'default' => null,
            'default' => false
        ]);
        Args::checkKey($options, 'secure', 'boolean', [
            'required' => true
        ]);
        Args::checkKey($options, 'httpOnly', 'boolean', [
            'label' => 'httponly flag',
            'default' => false
        ]);
        Args::checkKey($options, 'encoded', 'boolean', [
            'label' => 'encoded flag',
            'default' => false
        ]);
        $this->client->getCookieJar()->set(
            new \Symfony\Component\BrowserKit\Cookie(
                    $options['name'], $options['value'],
                    $options['expires'], $options['path'],
                    $options['domain'], $options['secure'],
                    $options['httpOnly'], $options['encoded']
                )
        );
    }

    /**
     * Returns the currently selected form, if a form has been selected,
     * and the first form in the current pape, otherwise. In the latter case,
     * selects the returned form as the current form.
     *
     * @return Symfony\Component\DomCrawler\Form
     * @throw CodeRage\Error if the page stack is empty or if the current page
     *   contains no forms
     */
    public function form()
    {
        if ($this->form === null)
            $this->form = $this->defaultForm();
        return $this->form;
    }

    /**
     * Sets the current form
     *
     * @param array $options An associative array with exactly one of the
     *   following keys:
     *     name - The form name
     *     id - The form ID
     *     selector - A CSS selector
     *     xpath - An XPath expression
     * @throws CodeRage\Error if the page stask is empty or if no form
     *   satisfies the specified creiteria
     */
    public function setForm(array $options)
    {
        $opt = Args::uniqueKey($options, ['name', 'id', 'selector', 'xpath']);
        $value = $options[$opt];
        Args::check($value, 'string', $opt);
        if ( ($opt == 'name' || $opt == 'id') &&
             !preg_match(Constants::MATCH_ATTRIBUTE, $value) )
        {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' => "Invalid $opt: $value"
                ]);
        }
        $crawler = $this->crawler();
        if ($opt === 'xpath') {
            $crawler = $crawler->filterXPath($value);
        } else {
            $selector = $opt == 'name' ?
                "form[name=\"$value\"]" :
                ( $opt == 'id' ?
                      "form[id=\"$value\"]" :
                      $value );
            $crawler = $crawler->filter($selector);
        }
        if ($crawler->count() == 0)
            $this->wrongPage('No form satisfies the specified creiteria');
        if ($crawler->getNode(0)->localName != 'form')
            $this->wrongPage('No specified element is not a form');
        $this->form = $crawler->form();
    }

    /**
     * Returns the a field with the given name exists in the currently selected
     * form, or in the first form in the current page if no form is selected
     *
     * @param string $name The field name
     * @return boolean
     * @throws CodeRage\Error if the page stack is empty or if the current page
     *   contains no forms
     */
    public final function hasField($name)
    {
        $form = $this->form();
        try {
            $form->get($name);
            return true;
        } catch (InvalidArgumentException $e) {
            return false;
        }
    }

    /**
     * Returns the named field in the currently selected form, or in the first
     * form in the current page if no form is selected
     *
     * @param string $name The field name
     * @return Symfony\Component\DomCrawler\Field\FormField The field
     * @throws CodeRage\Error if the page stack is empty, if the current page
     *   contains no forms or if no field with the given name exists
     */
    public final function field($name)
    {
        try {
            return $this->form()->get($name);
        } catch (InvalidArgumentException $e) {
            $fields =
                Array_::map(function($f) { return $f->getName(); }, $this->form()->all(), ', ');
            $this->wrongPage([
                'details' =>
                    "The current form has no field '$name': value fields " .
                    "are $fields",
                'inner' => $e
            ]);
        }
    }

    /**
     * Returns the value of the named field in the currently selected form, or
     * in the first form in the current page if no form is selected. File inputs
     * are not supported.
     *
     * @param string $name The field name
     * @return mixed value The field value
     * @throws CodeRage\Error if the page stack is empty, if the current page
     *   contains no forms, if no field with the given name exists, or if the
     *   named field is a file upload field
     */
    public final function fieldValue($name)
    {
        $field = $this->field($name);
        if ($field instanceof \Symfony\Component\DomCrawler\Field\FileFormField)
            $this->wrongPage("The field '$name' is a file input");
        return $this->field($name)->getValue();
    }

    /**
     * Alias of fieldValue()
     */
    public final function value($name)
    {
        return $this->fieldValue($name);
    }

    /**
     * Sets the specified fields of the given form
     *
     * @param array $fields An associative array mapping field names to field
     *   values. Field values may be
     *     - a string (the most common case)
     *     - a list of strings (for fields with multiple values)
     *     - a list of associative arrays with keys 'path', 'filename', and
     *       'contentType' (for file inputs)
     * @param Symphony\Component\DomCrawler\Form $form The form; defaults to the
     *   current form
     * @throws CodeRage\Error if the page stask is empty, if the current page
     *   contains no forms, if the current form has no field with one of the
     *   specified names, or if one of the assigned values is invalid for the
     *   specified field
     */
    public final function setFields(array $fields, ?Form $form = null)
    {
        if ($form === null)
            $form = $this->form();
        Args::check($fields, 'map', 'fields');
        [$params, $files] = $this->processFields($fields);
        foreach ($files as $n => $v) {
            $this->setFileUploadField(
                $n,
                $v['path'],
                $v['filename'],
                $v['contentType']
            );
        }
        foreach ($params as $n => $v) {
            if (!$form->has($n))
                $this->wrongPage("Form has no field '$n'");
            try {
                $form[$n] = $v;
            } catch (InvalidArgumentException $e) {
                if (!is_scalar($v))
                    $v = json_encode($v);
                $this->wrongPage([
                    'details' => "$v is not a valid value for field '$n'",
                    'inner' => $e
                ]);
            }
        }
    }

    /**
     * Sets the value of an input of type "file"
     *
     * @param string $name The name of the input
     * @param string $path The file path
     * @param string $filename The file name
     * @param string $contentType The MIME media type
     */
    public final function setFileUploadField($name, $path, $filename, $contentType)
    {
        Args::check($name, 'string', 'name');
        if (!$this->form()->has($name))
            $this->wrongPage("The current form has no field '$name'");
        $field = $this->form()[$name];
        if (!$field instanceof FileFormField)
            $this->wrongPage("The field '$name' is not a file upload field");
        FileUploadFieldSetter::set($field, $path, $filename, $contentType);
    }

    /**
     * Performs a GET request, making multiple attempts if necessary
     *
     * @param mixed $uri The request URI, as a string or as an instance of
     *   Psr\Http\Message\UriInterface
     * @param array $options An associative array with keys among:
     *     errorMessage - An error message used to construct an exception if the
     *       request fails (optional)
     *     test - A test to apply to determine if the request was successful;
     *       may be a regular expression to match against the response body or a
     *       callable taking an instance of Psr\Http\Message\ResponseInterface
     *       and throwing exception on failure (optional)
     *     headers - An associative array of HTTP headers (optional)
     *     outputFile - A file path to which the response body should be written
     *       (optional)
     * @return Psr\Http\Message\ResponseInterface
     */
    public final function get($uri, array $options = [])
    {
        $request =
            function() use($uri)
            {
                return $this->client->request('GET', (string)$uri);
            };
        $options += ['method' => 'GET', 'uri' => $uri];
        return $this->doRequest($request, $options);
    }

    /**
     * Performs a POST request, making multiple attempts if necessary
     *
     * @param mixed $uri The request URI, as a string or as an instance of
     *   Psr\Http\Message\UriInterface
     * @param array $options An associative array with keys among:
     *     postData - An associative array mapping field names to field values.
     *       Field values may be
     *         - a string (the most common case)
     *         - a list of strings (for fields with multiple values)
     *         - a list of associative arrays with keys 'path', 'filename', and
     *       'contentType' (for file inputs)
     *     body - The request body (optional)
     *     errorMessage - An error message used to construct an exception if the
     *       request fails (optional)
     *     test - A test to apply to determine if the request was successful;
     *       may be a regular expression to match against the response body or a
     *       callable taking an instance of Psr\Http\Message\ResponseInterface
     *       and throwing exception on failure (optional)
     *     headers - An associative array of HTTP headers (optional)
     *     multipart - true to encode form data as multipart/form-data
     *     outputFile - A file path to which the response body should be written
     *       (optional)
     *   At most one of the options 'postData' and 'content' may be specified.
     * @return Psr\Http\Message\ResponseInterface
     */
    public final function post($uri, array $options = [])
    {
        $this->processPostOptions($options);
        $request =
            function() use($uri, $options)
            {
                return $this->client->request(
                           'POST',
                           (string)$uri,
                           $options['params'],
                           $options['files'],
                           [],
                           $options['body']
                       );
            };
        $options += ['method' => 'POST', 'uri' => $uri];
        return $this->doRequest($request, $options);
    }

    /**
     * Submits the current form, making multiple attempts if necessary
     *
     * @param array $options An associative array with keys among:
     *     fields - An associative array mapping field names to field values.
     *       Field values may be
     *         - a string (the most common case)
     *         - a list of strings (for fields with multiple values)
     *         - a list of associative arrays with keys 'path', 'filename', and
     *       'contentType' (for file inputs)
     *     buttonName - The name of the button to click (optional)
     *     buttonId - The ID of the button to click (optional)
     *     buttonValue - The value of the button's "value" attribute (optional)
     *     buttonIndex - The 1-based position of the button to click (optional)
     *     formName - The form name (optional)
     *     formId - The form ID (optional)
     *     formSelector - A CSS selector (optional)
     *     formXpath - An XPath expression (optional)
     *     errorMessage - An error message used to construct an exception if the
     *       request fails (optional)
     *     test - A test to apply to determine if the request was successful;
     *       may be a regular expression to match against the response body or a
     *       callable taking an instance of Psr\Http\Message\ResponseInterface
     *       and throwing exception on failure (optional)
     *     headers - An associative array of HTTP headers (optional)
     *     outputFile - A file path to which the response body should be writte
     *       (optional)
     *   At most one of the "buttonName", "buttonId" and "buttonIndex" may be
     *   specified; the same is true of "formName", "formId", "formSelector",
     *   and "formXpath"
     * @return Psr\Http\Message\ResponseInterface
     */
    public final function submit(array $options = [])
    {
        // Set current form
        if ( isset($options['formName']) ||
             isset($options['formId']) ||
             isset($options['formSelector']) ||
             isset($options['formXpath']) )
        {
            $this->setForm([
                'name' => $options['formName'] ?? null,
                'id' => $options['formId'] ?? null,
                'selector' => $options['formSelector'] ?? null,
                'xpath' => $options['formXpath'] ?? null
            ]);
        }

        // Handle button options
        $form = $this->formFromButton($options);
        if ($form === null)
            $form = $this->form();

        // Set form fields
        if (isset($options['fields']))
            $this->setFields($options['fields'], $form);

        // Set multipart options
        if ( $form->getNode()->hasAttribute('enctype') &&
             $form->getNode()->getAttribute('enctype') == 'multipart/form-data' )
        {
            $options['multipart'] = true;
        }

        // Submit request
        $request =
            function() use ($form)
            {
                return $this->client->submit($form);
            };
        $options += ['method' => $form->getMethod(), 'uri' => $form->getUri()];
        return $this->doRequest($request, $options);
    }

    /**
     * Pops the page stack
     */
    public final function back()
    {
        $this->client->back();
    }

            /*
             * Utility methods
             */

    /**
     * Stores this robot's content, or the specified string, in a file and
     * throws an exception with a message referencing the file path. May be
     * called with an error message as argument or with an options array.
     *
     * @param mixed $messageOrOptions an error message, as a string, or an
     *   options array supporting the following options:
     *     status - The error status; defaults to UNEXPECTED_CONTENT
     *     message - An error messag (optional)
     *     details - A detailed error message (optional)
     *     inner - The inner exception (optional)
     *     content - The string to store; defaults to this robot's content
     *     contentType - The MIME media type of the string to store; defaults to
     *       this robot's content type
     * @throws CodeRage\Error
     */
    public final function wrongPage($messageOrOptions)
    {
        $options = is_string($messageOrOptions) ?
            $options = ['message' => $messageOrOptions] :
            $messageOrOptions;
        Args::checkKey($options, 'status', 'string', [
            'default' => 'UNEXPECTED_CONTENT'
        ]);
        foreach (['message', 'details', 'content', 'contentType'] as $opt)
            Args::checkKey($options, $opt, 'string', ['default' => null]);
        Args::checkKey($options, 'inner', 'Throwable', [
            'default' => null
        ]);
        $prefix = $options['details'] ?? $options['message'] ?? null;
        if ($prefix !== null || $this->contentRecorder !== null) {
            $location = $this->contentRecorder !== null ?
                $this->recordContent($options['content'], $options['contentType']) :
                null;
            $options['details'] = $prefix !== null && $location !== null ?
                "$prefix: see $location" :
                ($prefix !== null ? $prefix : "See $location");
        } else {
            $options['details'] = null;
        }
        throw new
            Error([
                'status' => $options['status'],
                'message' => $options['message'],
                'details' => $options['details'],
                'inner' => $options['inner']
            ]);
    }

    /**
     * Stores this robot's content, or the specified string, and returns the
     * location of the stored data. By default, stores the content in a file and
     * returns the file path; to change this behavior, use the method
     * setContentRecorder().
     *
     * @param string $content The string to store; defaults to this robot's
     *   content
     * @param string $contentType The MIME media type of $content; defaults to
     *   this robot's content type
     * @return string The location of the stored data
     */
    public final function recordContent($content = null, $contentType = null)
    {
        if ($this->contentRecorder === null)
            throw new
                Error([
                    'status' => 'STATE_ERROR',
                    'details' => 'The content recorder has not been set'
                ]);
        if ($content === null)
            $content = $this->content();
        if ($contentType === null)
            $contentType = $this->contentType();
        return $this->contentRecorder->recordContent($content, $contentType);
    }


    /**
     * Returns the solution to the CAPTCHA challenge associated with the current
     * form
     *
     * @return array An associative array with keys among:
     *     fields - An associative array mapping form field names to strings or
     *       lists of strings (optional)
     *     headers - An associative array of HTTP headers (optional)
     *     metadata - An associative array of additional data obtained during
     *       CAPTCHA solving (optional)
     *     class - The class name of the CAPCTHA solver that solved the CAPTCHA
     * @throws Exception if a solution could not be found
     */
    public function solveCaptcha(): array
    {
        foreach ($this->captchaSolvers as $solver) {
            if ($solver->canSolve($this)) {
                $result = $solver->solve($this);
                $result['class'] = get_class($solver);
                return $result;
            }
        }
        throw new
            Error([
                'status' => 'OBJECT_DOES_NOT_EXIST',
                'details' => 'No CAPTCHA solver can solve the CAPTCHA challenge'
            ]);
    }

    /**
     * Repeatedly invokes the given operation, using an exponential backoff
     * strategy
     *
     * @param callable $operation The operation to invoke
     * @param callable $handleError A callable taking a single argument
     *   to be called each time $operation throws an exception, returning true
     *   if $operation should be attempted again and false if the exception
     *   should  be re-thrown
     * @param string $message A participial phrase describing the
     *   operation
     */
    public final function repeatOperation($operation, $handleError, $message)
    {
        $backoff =
            new \CodeRage\Util\ExponentialBackoff([
                    'attempts' => $this->fetchAttempts,
                    'sleep ' => $this->fetchSleep / 10000000,
                    'multiplier' => $this->fetchMultiplier
                ]);
        $backoff->execute($operation, $handleError, $message, $this->log());
    }

    protected function doExecute(array $options) { }


    /**
     * Helper method for get, post, and submit
     *
     * @param callable $request A callable taking an instance of
     *   CodeRage\Tool\Robot and an associative array of options
     * @param array $options An options array; supports the following options:
     *     method - The HTTP request method, for request logging
     *     uri - The URI, as a string or instance of
     *       Psr\Http\Message\UriInterface
     *     errorMessage - An error message for use in constructing an exception
     *       if the request is not succfessful after multiple attempts
     *       (optional)
     *     test - A test to apply to determine if the request was successful;
     *       may be a regular expression to match against the response body, an
     *       expected HTTP status code, or a callable taking an instance of
     *       Psr\Http\Message\ResponseInterface and throwing exception on
     *       failure (optional)
     *     headers - An associative array of HTTP headers (optional)
     *     multipart - true to encode form data as multipart/form-data
     *     outputFile - A file path to which the response body should be written
     *       (optional)
     * @return mixed
     */
    private function doRequest($request, $options)
    {
        // Process options
        Args::check($request, 'callable', 'request');
        Args::checkKey($options, 'method', 'string', [
            'required' => true
        ]);
        Args::checkKey($options, 'uri', 'string|Psr\\Http\\Message\\UriInterface', [
            'label' => 'URI',
            'required' => true
        ]);
        Args::checkKey($options, 'errorMessage', 'string', [
            'label' => 'error message',
            'default' => null
        ]);
        Args::checkKey($options, 'test', 'int|regex|callable', [
            'default' => null
        ]);
        Args::checkKey($options, 'multipart', 'boolean', [
            'default' => false
        ]);
        Args::checkKey($options, 'headers', 'map');
        Args::checkKey($options, 'outputFile', 'string', [
            'label' => 'output file'
        ]);
        if ($options['method'] == 'GET' && $options['multipart'])
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' =>
                        'GET requests may not be encoded as ' .
                        'multipart/form-data'
                ]);

        // Set Guzzle request options
        $requestOptions = $this->requestOptions;
        if (isset($options['outputFile']))
            $requestOptions['sink'] = $options['outputFile'];
        if (isset($options['headers']))
            $requestOptions['headers'] =
                $options['headers'] + $requestOptions['headers'];
        $this->client->setRequestOptions($requestOptions);
        $this->client->setMultipart($options['multipart']);

        // Define input to doRequest()
        $method = $options['method'];
        $uri = is_string($options['uri'])?
            new Uri($options['uri']) :
            $options['uri'];
        $errorMessage = $options['errorMessage'];
        $test = $options['test'];
        $operation =
            function() use($request, $method, $uri, $errorMessage, $test)
            {
                try {
                    foreach ($this->requestLoggers as $logger)
                        $logger->preRequest($this, $method, $uri);
                    $request();
                    foreach ($this->requestLoggers as $logger)
                        $logger->postRequest($this, $method, $uri);
                } catch (Throwable $e) {
                    throw $this->translateClientError($e);
                }
                if ($stream = $this->log()->getStream(Log::DEBUG)) {
                    $details = $this->recordContent();
                    $stream->write("Content of HTTP response: see $details");
                }
                $code = $this->response()->getStatusCode();
                if (!is_int($test) && ($code < 200 | $code >= 300))
                    $this->wrongPage([
                        'status' => 'UNEXPECTED_BEHAVIOR',
                        'message' =>
                            $this->composeError(
                                $errorMessage,
                                "Unexpected HTTP status $code"
                            )
                    ]);
                if (is_string($test) && !preg_match($test, $this->content())) {
                    $this->wrongPage(['message' => $errorMessage]);
                } elseif (is_int($test) && $code != $test) {
                    $this->wrongPage([
                        'status' => 'UNEXPECTED_BEHAVIOR',
                        'message' =>
                            $this->composeError(
                                $errorMessage,
                                "Unexpected HTTP status: expected $test; found $code"
                            )
                    ]);
                } elseif (is_callable($test)) {
                   $test($this->response());
                }
            };
        $handleError =
            function($error) use($method, $uri, $test)
            {
                foreach ($this->requestLoggers as $logger)
                    $logger->postRequest($this, $method, $uri, $error);
                if ($this->response() === null)
                    return true;
                if ($error instanceof Error) {
                    if ($error->status() == 'UNEXPECTED_CONTENT')
                        return false;
                    if ($error->status() == 'HTTP_ERROR') {
                        $code = $this->response()->getStatusCode();
                        return $code >= 400 && $code < 500 && $code != 408 ||
                               $code == 501 || $code == 505;
                    }
                }
                return $error instanceof Error && $error->status() == 'RETRY';
            };

        // Execute operation
        $this->crawler =
            $this->repeatOperation(
                $operation,
                $handleError,
                'performing operation'
            );
        $this->form = null;
        return $this->response();
    }

    /**
     * Returns an instance of CodeRage\Error constructed from the given error
     * message thrown by the BrowserKit client
     *
     * @return CodeRage\Error
     */
    private function translateClientError($error)
    {
        $status = null;
        if ($error instanceof \GuzzleHttp\Exception\ConnectException) {
            $status = 'HOST_UNREACHABLE';
        } elseif ($error instanceof \GuzzleHttp\Exception\GuzzleException) {
            $status = 'HTTP_ERROR';
        }
        throw new Error(['status' => $status, 'inner' => $error]);
    }

    /**
     * Combines the two given strings to form an error messages
     *
     * @param string $prefix The prefix, possibly null
     * @param string $body The principal portion of the error message
     * @return CodeRage\Error
     */
    private function composeError($prefix, $body)
    {
        return $prefix !== null ? "$prefix: " . lcfirst($body) : $body;
    }

    /**
     * Returns the value of the named Guzzle request option, if any
     *
     * @param string $name The option name
     * @return mixed
     */
    private function getRequestOption($name)
    {
        Args::check($name, 'string', 'request option');
        return $this->requestOptions[$name] ?? null;
    }

    /**
     * Sets the value of the named Guzzle request option
     *
     * @param string $name The option name
     * @param mixed $value The option value
     * @throws CodeRage\Error if the named option is non-configurable
     */
    private function setRequestOption($name, $value)
    {
        Args::check($name, 'string', 'request option');
        if (array_key_exists($name, Constants::NON_CONFIGURATBLE_REQUEST_OPTIONS))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'details' =>
                        "The request option '$name' is non-configurable"
                ]);
        if ($name == 'headers')
            Args::check($value, 'array', 'headers');
        if ($name == 'curl')
            Args::check($value, 'array', 'cURL options');
        $this->requestOptions[$name] = $value;
    }

    /**
     * Clears the value of the named Guzzle request option
     *
     * @param string $name The option name
     */
    private function clearRequestOption($name)
    {
        Args::check($name, 'string', 'request option');
        unset($this->requestOptions[$name]);
    }

    /**
     * Validates and processes options for post()
     *
     * @param array $options An associative array with keys among:
     *     postData - An associative array mapping field names to field values.
     *       Field values may be
     *         - a string (the most common case)
     *         - a list of strings (for fields with multiple values)
     *         - a list of associative arrays with keys 'path', 'filename', and
     *       'contentType' (for file inputs)
     *     body - The request body (optional)
     *     errorMessage - An error message used to construct an exception if the
     *       request fails (optional)
     *     test - A test to apply to determine if the request was successful;
     *       may be a regular expression to match against the response body or a
     *       callable taking an instance of Psr\Http\Message\ResponseInterface
     *       and throwing exception on failure (optional)
     *     headers - An associative array of HTTP headers (optional)
     *     multipart - true to encode form data as multipart/form-data
     *     outputFile - A file path to which the response body should be written
     *       (optional)
     *   At most one of the options 'postData' and 'content' may be specified.
     */
    private function processPostOptions(array &$options)
    {
        $postData =
            Args::checkKey($options, 'postData', 'map', [
                'label' => 'post data'
            ]);
        $body = Args::checkKey($options, 'body', 'string');
        if ($postData !== null and $body !== null) {
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' => "The options 'postData' and 'body' are incompatible"
                ]);
        }
        if ($body !== null) {
            if ($options['multipart'] ?? false) {
                throw new
                    Error([
                        'status' => 'INCONSISTENT_PARAMETERS',
                        'details' =>
                            "The options 'body' and 'multipart' are incompatible"
                    ]);
            }
            $options['params'] = [];
            $options['files'] = [];
        } else {
            [$params, $files] = $this->processFields($options['postData'] ?? []);
            foreach ($files as $n => $v) {
                $files[$n] =
                    [
                        'tmp_name' => $v['path'],
                        'name' => $v['filename'],
                        'type' => $v['contentType'],
                        'size' => filesize($v['path']),
                        'error' => UPLOAD_ERR_OK
                    ];
            }
            $options['params'] = $params;
            $options['files'] = $files;
            $options['body'] = null;
        }
    }

    /**
     * Returns the first form in the current page
     *
     * @return Symfony\Component\DomCrawler\Form
     * @throw CodeRage\Error if the page stack is empty or if the current page
     *   contains no forms
     */
    private function defaultForm()
    {
        $crawler = $this->crawler()->filter('form');
        if ($crawler->count() == 0)
            $this->wrongPage('The current page contains no forms');
        return $crawler->form();
    }

    /**
     * Returns a form which has a button matching the given criteria, if one
     * exists, and which is submitted by clicking the button
     *
     * @param array $options An associative array with keys among:
     *     buttonName - The name of the button to click (optional)
     *     buttonId - The ID of the button to click (optional)
     *     buttonValue - The value of the button's "value" attribute (optional)
     *     buttonIndex - The 1-based position of the button to click (optional)
     *   Exactly one of the "buttonName", "buttonId" and "buttonIndex" must be
     *   supplied
     * @return Symfony\Component\DomCrawler\Form
     * @throws CodeRage\Error if the options are invalid or if no such form
     *   exists
     */
    private function formFromButton(array $options)
    {
        $opt = $this->processButtonOptions($options);
        if ($opt === null)
            return null;
        [$name, $value] = $opt;
        $button = $name == 'buttonIndex' ?
            $this->buttonFromIndex($value) :
            $this->buttonFromAttribute(
                Constants::BUTTON_OPTIONS[$name]['attribute'],
                $value
            );
        $crawler = $this->crawler();
        $form =
            new Form($button, $crawler->getUri(), null, $crawler->getBaseHref());
        foreach ($this->form->all() as $n => $lhs) {
            $rhs = $form[$n];
            if ($lhs instanceof FileFormField) {
                FileUploadFieldSetter::copy($lhs, $rhs);
            } elseif ($rhs->hasValue()) {
                try {
                    $rhs->setValue($lhs->getValue());
                } catch (InvalidArgumentException $e) {
                    $this->wrongPage([
                        'details' => "Failed setting field '$n'",
                        'inner' => $e
                    ]);
                }
            }
        }
        return $form;
    }

    /**
     * Validates options for fmFromButton()
     *
     * @param array $options The options array passed to formFromButton()
     * @return array A pair [$name, $value] where $name is name of the button
     *   option and $value is its value
     */
    private function processButtonOptions(array &$options)
    {
        $result = null;
        foreach (Constants::BUTTON_OPTIONS as $name => $opts) {
            $label = $opts['label'];
            $value =
                Args::checkKey($options, $name, 'string', [
                    'label' => "button $label"
                ]);
            if ($value !== null) {
                $attr = $opts['attribute'];
                if ( ($attr == 'name' || $attr == 'id') &&
                      !preg_match(Constants::MATCH_ATTRIBUTE, $value) )
                {
                    throw new
                        Error([
                            'status' => 'INVALID_PARAMETER',
                            'details' => "Invalid button $label: $value"
                        ]);
                }
                if ($result !== null)
                    throw new
                        Error([
                            'status' => 'INCONSISTENT_PARAMETERS',
                            'details' => 'Multiple button options specified'
                        ]);
                $result = [$name, $value];
            }
        }
        $index =
            Args::checkKey($options, 'buttonIndex', 'int', [
                'label' => 'button index'
            ]);
        if ($index !== null) {
            if ($index <= 0)
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'details' => "Invalid button index: $index"
                    ]);
            if ($result !== null)
                throw new
                    Error([
                        'status' => 'INCONSISTENT_PARAMETERS',
                        'details' => 'Multiple button options specified'
                    ]);
            $result = ['buttonIndex', $index];
        }
        return $result;
    }

    /**
     * Returns the button in the current response with the specified attribute
     * value
     *
     * @param string $name The attribute name
     * @param string $value The attribute value
     *
     * @return DOMElement
     */
    private function buttonFromAttribute($name, $value)
    {
        $context = $this->form()->getFormNode();
        $xpath = new DOMXpath($context->ownerDocument);
        $type =
            'translate(@type, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")';
        $value = Crawler::xpathLiteral($value);
        $expr =
            "descendant-or-self::input[($type='button' or $type='submit') and @$name=$value] | " .
            "descendant-or-self::button[@$name=$value]";
        $nodes = $xpath->query($expr, $context);
        if ($nodes->length > 0) {
            return $nodes->item(0);
        } else {
            $this->wrongPage("Form has no button $name '$value'");
        }
    }

    /**
     * Returns the button in the current response with the specified position
     * in the collection of all buttons
     *
     * @param DOMDocument $dom The document
     * @param int $index The 1-based position
     *
     * @return DOMElement
     */
    private function buttonFromIndex($index)
    {
        $context = $this->form()->getFormNode();
        $xpath = new DOMXpath($context->ownerDocument);
        $type =
            'translate(@type, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")';
        $expr =
            "(descendant-or-self::input[$type='button' or $type='submit']| " .
            " descendant-or-self::button)[position()=$index]";
        $nodes = $xpath->query($expr, $context);
        if ($nodes->length > 0) {
            return $nodes->item(0);
        } else {
            $this->wrongPage("Form has no button with index $index");
        }
    }

    /**
     * Validates POST fields and returns a tuple [$params, $files], where
     * $params is an associative array of POST fields for which value are
     * either string or an array and $files is an associative array of POST
     * fields for which value is an associative array with keys amoung 'path',
     * 'filename' and 'contentType'
     *
     * @param array $fields An associative array mapping field names to field
     *   values. Field values may be
     *     - a string (the most common case)
     *     - a list of strings (for fields with multiple values)
     *     - a list of associative arrays with keys 'path', 'filename', and
     *       'contentType' (for file inputs)
     * @return array
     * @throws CodeRage\Error
     */
    private function processFields(array $fields)
    {
        $params = $files = [];
        foreach ($fields as $n => $v) {
            Args::check($v, 'boolean|int|string|array', "value of POST parameter '$n'");
            if (is_scalar($v)) {
                $params[$n] = is_bool($v) ? $v : (string) $v;
            } elseif (Array_::isIndexed($v)) {
                foreach ($v as $i => $v1) {
                    Args::check($v1, 'int|string', "value at index $i of POST parameter '$n'");
                    $v[$i] = (string) $v1;
                }
                $params[$n] = $v;
            } else {
                Args::checkKey($v, 'path', 'string', [
                    'required' => true
                ]);
                Args::checkKey($v, 'filename', 'string', [
                    'label' => 'file name',
                    'required' => true
                ]);
                Args::checkKey($v, 'contentType', 'string', [
                    'label' => 'content type',
                    'required' => true
                ]);
                $files[$n] = $v;
            }
        }
        return [$params, $files];
    }

    /**
     * The number of times to attempt a request before aborting
     *
     * @var int
     */
    private $fetchAttempts;

    /**
     * The number of seconds to wait after an initial failed request before
     * a second attempt
     *
     * @var float
     */
    private $fetchSleep;

    /**
     * The multipler for use in exponential backoff when a request fails; used
     * in conjunction with $fetchAttempts $fetchSleep
     *
     * @var float
     */
    private $fetchMultiplier;

    /**
     * A list of instances of CodeRage\Tool\Robot\RequestLogger
     *
     * @var array
     */
    private $requestLoggers = [];

    /**
     * The content recorder
     *
     * @var CodeRage\Tool\Robot\ContentRecorder
     */
    private $contentRecorder;

    /**
     * A list of instances of CodeRage\Tool\Robot\CaptchaSolver
     *
     * @var array
     */
    private $captchaSolvers = [];

    /**
     * An assoiciatve array of Guzzle options to be passed to the BrowserKit
     * client with each request
     *
     * @array
     */
    private $requestOptions = [];

    /**
     * The Symfony BrowserKit client
     *
     * @var CodeRage\Tool\Robot\BrowserKitClient
     */
    private $client;

    /**
     * The currently selected HTML form, if any
     *
     * @var Symfony\Component\DomCrawler\Form
     */
    private $form;
}
