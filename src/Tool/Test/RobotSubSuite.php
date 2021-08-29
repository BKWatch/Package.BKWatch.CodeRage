<?php

/**
 * Defines the class CodeRage\Tool\Test\RobotSubSuite
 *
 * File:        CodeRage/Tool/Test/RobotSuite.php
 * Date:        Mon Apr 16 11:12:16 UTC 2018
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2018 CounselNow, LLC
 * @author      Fallak Asad
 * @license     All rights reserved
 */

namespace CodeRage\Tool\Test;

use Throwable;
use CodeRage\Config;
use CodeRage\Error;
use CodeRage\File;
use CodeRage\Test\Assert;
use CodeRage\Text\Regex;
use CodeRage\Tool\BasicRobot;
use CodeRage\Util\Args;
use CodeRage\Util\BracketObjectNotation;
use CodeRage\Util\Json;
use CodeRage\Util\Time;

/**
 * Test suite for robots constructed with a specified collection of options
 */
class RobotSubSuite extends \CodeRage\Test\ReflectionSuite {

    /**
     * @var string
     */
    private const MOCK_RESPONSE = '/CodeRage/Tool/Test/mock-response.php';

    /**
     * @var string
     */
    private const HTTP_ECHO_DOMAIN = 'httpecho.org';

    /**
     * @var string
     */
    private const MATCH_DATA_URI = '/^data:(.+?)?(;base64)?,(.*)$/';

    /**
     * @var string
     */
    private const MATCH_MULTIPART_FORM_DATA = '/^multipart\/form-data\b/';

    /**
     * @var array
     */
    private const HTTP_ECHO_PROPERTIES =
        [
            'uri' => ['URI', 'string', true],
            'method' => ['method', 'string', true],
            'headers' => ['headers', 'map[string]', true],
            'contentType' => ['content type', 'string', false],
            'cookies' => ['cookies', 'map[string]', false],
            'form' => ['form data', 'list[map]', false],
        ];

    /**
     * @var array
     */
    private const FORM_FIELD_PROPERTIES =
        [
            'name' => ['name', 'string', true],
            'value' => ['value', 'string', true],
            'contentType' => ['content type', 'string', false],
            'filename' => ['file name', 'string', false]
        ];

    /**
     * Constructs an instance of CodeRage\Tool\Test\RobotSuite
     *
     * @param array $options The options array; supports the following options:
     *     name - The test suite name
     *     description - The test suite description
     *     robotOptions - The robot constructor options; defaults to an empty
     *       array
     *     skipLongCases - true to skip test cases that take substantially
     *       longer than most other tests; defaults to false
     *
     */
    public function __construct(array $options = [])

    {
        $name =
            Args::checkKey($options, 'name', 'string', [
                'required' => true
            ]);
        $description =
            Args::checkKey($options, 'description', 'string', [
                'required' => true
            ]);
        $robotOptions =
            Args::checkKey($options, 'robotOptions', 'map', [
                'default' => []
            ]);
        $skipLongCases =
            Args::checkKey($options, 'skipLongCases', 'boolean', [
                'default' => false
            ]);
        parent::__construct($name, $description);
        $this->robotOptions = $robotOptions;
        $this->skipLongCases = $skipLongCases;
    }

            /*
             * Test Robot construct with various options
             */

    /**
     * Tests constructing a robot with all possible options
     */
    public function testConstructWithAllOptions()
    {
        new BasicRobot([
                'userAgent' => 'Mozilla/5.0',
                'accept' => 'text/xml',
                'acceptLanguage' => 'en-us',
                'timeout' => 50,
                'verifyCertificate' => true,
                'fetchAttempts' => 2,
                'fetchSleep' => 20,
                'fetchMultipler' => 2.0
            ]);
    }

    /**
     * Tests constructing a robot with invalid "userAgent" option
     */
    public function testConstructWithInvalidUserAgentOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = new BasicRobot(['userAgent' => 0]);
    }

    /**
     * Tests constructing a with invalid "accept" option
     */
    public function testConstructWithInvalidAcceptOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = new BasicRobot(['accept' => 0]);
    }

    /**
     * Tests creating robot with invalid "acceptLanguage" option
     */
    public function testConstructWithInvalidAcceptLanguageOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = new BasicRobot(['acceptLanguage' => 0]);
    }

    /**
     * Tests constructing a robot with invalid "timeout" option
     */
    public function testConstructWithInvalidTimeoutOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = new BasicRobot(['timeout' => 'XX']);
    }

    /**
     * Tests constructing a robot with invalid "verifyCertificate" option
     */
    public function testConstructWithInvalidVerifyCertificateOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = new BasicRobot(['verifyCertificate' => 'XX']);
    }

    /**
     * Tests constructing a robot with invalid "fetchAttempts" option
     */
    public function testConstructWithInvalidFetchAttemptsOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = new BasicRobot(['fetchAttempts' => 'XX']);
    }

    /**
     * Tests constructing a robot with invalid "fetchSleep" option
     */
    public function testConstructWithInvalidFetchSleepOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = new BasicRobot(['fetchSleep' => 'XX']);
    }

    /**
     * Tests constructing a robot with invalid "fetchMultipler" option
     */
    public function testConstructWithInvalidFetchMultiplerOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = new BasicRobot(['fetchMultiplier' => 'XX']);
    }

        /*
         * Test fetchAttempts() and setFetchAttempts() methods
         */

    /**
     * Tests fetchAttempts() and setFetchAttempts()
     */
    public function testSetAndGetFetchAttempts()
    {
        $robot = $this->createRobot();
        $robot->setFetchAttempts(3);
        Assert::equal($robot->fetchAttempts(), 3, 'Fetch attempt');
    }

    /**
     * Tests setFetchAttempts() with non-positive value
     */
    public function testSetFetchAttemptsWithNonPositiveNumberFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setFetchAttempts(-3);
    }

    /**
     * Tests setFetchAttempts() with string value
     */
    public function testSetFetchAttemptsWithStringFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setFetchAttempts('X');
    }

        /**
         * Test fetchSleep() and setFetchSleep() methods
         */

    /**
     * Tests fetchSleep() and setFetchSleep()
     */
    public function testSetAndGetFetchSleep()
    {
        $robot = $this->createRobot();
        $robot->setFetchSleep(300);
        Assert::equal($robot->fetchSleep(), 300, 'sleep time');
    }

    /**
     * Tests setFetchSleep() with non-positive value
     */
    public function testSetFetchSleepWithNonPositiveValueFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setFetchSleep(-300);
    }

    /**
     * Tests setFetchSleep() with string value
     */
    public function testSetFetchSleepWithStringFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setFetchSleep('X');
    }

        /*
         * Test fetchMultiplier() and setFetchMultiplier() methods
         */

    /**
     * Tests fetchMultiplier() and setFetchMultiplier()
     */
    public function testSetAndGetFetchMultiplier()
    {
        $robot = $this->createRobot();
        $robot->setFetchMultiplier(30.0);
        Assert::equal($robot->fetchMultiplier(), 30.0, 'time multipler');
    }

    /**
     * Tests setFetchMultiplier() with non-positive value
     */
    public function testSetFetchMultiplierWithNonPositiveValueFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setFetchMultiplier(-30);
    }

    /**
     * Tests setFetchMultiplier() with an integer
     */
    public function testSetFetchMultiplierWithIntegerFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setFetchMultiplier(3);
    }

    /**
     * Tests setFetchMultiplier() with string value
     */
    public function testSetFetchMultiplierWithStringFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setFetchMultiplier('X');
    }

        /**
         * Test timeout() and setTimeout() methods
         */

    /**
     * Tests timeout() and setTimeout()
     */
    public function testSetAndGetTimeout()
    {
        $robot = $this->createRobot();
        $robot->setTimeout(30);
        Assert::equal($robot->timeout(), 30, 'timeout');
    }

    /**
     * Tests setTimeout() with non-positive value
     */
    public function testSetTimeoutWithNonPositiveNumberFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setTimeout(-30);
    }

    /**
     * Tests setTimeout() with string value
     */
    public function testSetTimeoutWithInvalidNumberFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setTimeout('X');
    }

        /**
         * Test verifyCertificate() and setVerifyCertificate() methods
         */

    /**
     * Tests verifyCertificate() and setVerifyCertificate()
     */
    public function testSetAndGetVerifyCertificate()
    {
        $robot = $this->createRobot();
        $robot->setVerifyCertificate(true);
        Assert::equal($robot->verifyCertificate(), true, 'verify certificate flag');
        $robot->setVerifyCertificate(false);
        Assert::equal($robot->verifyCertificate(), false, 'verify certificate flag');
    }

    /**
     * Tests setVerifyCertificate() with string value
     */
    public function testSetVerifyCertificateWithInvalidFlagFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setVerifyCertificate('X');
    }

        /**
         * Test getHeader(), setHeader() and clearHeader() methods
         */

    /**
     * Tests getHeader(), setHeader() and clearHeader()
     */
    public function testSetGetAndClearHeader()
    {
        $robot = $this->createRobot();
        $name = 'Content-Type';
        $value = 'application/json';
        $robot->setHeader($name, $value);
        Assert::equal($robot->getHeader($name), $value, 'content type header');
        $robot->clearHeader($name);
        Assert::equal($robot->getHeader($name), null, 'content type header');
    }

    /**
     * Test setHeader() with non-string header name
     */
    public function testSetHeaderWithNonStringHeaderNameFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setHeader(1, 'application/json');
    }

    /**
     * Test setHeader() with non-string header value
     */
    public function testSetHeaderWithNonStringHeaderValueFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setHeader('content-type', 1);
    }

    /**
     * Test getHeader() with non-string header name
     */
    public function testGetHeaderWithNonStringHeaderNameFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->getHeader(1);
    }

    /**
     * Test clearHeader() with non-string header name
     */
    public function testClearHeaderWithNonStringHeaderNameFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->clearHeader(1);
    }

            /*
             * Test request logger management functions
             */

    /**
     * Tests requestLoggers(), registerRequestLogger() and
     * unregisterRequestLogger(). This function registers the new logger,
     * checks that it has been register, unregisters it, and verifies the count
     * of registered loggers.
     */
    public function testRegisterUnregisterAndListRequestLogger()
    {
        $robot = $this->createRobot();

        Assert::equal(count($robot->requestLoggers()), 0, 'logger count');

        // Register new request logger
        $mrl =
            new \CodeRage\Tool\Test\MockRequestLogger(
                [function() {echo "1st pre request function\n";}],
                [function() {echo "1st post request function\n";}]
            );
        $robot->registerRequestLogger($mrl);

        // Check that $mrl is the most-recently registered request logger
        $loggers = $robot->requestLoggers();
        $lastLogger = $loggers[count($loggers) - 1];
        if ($lastLogger !== $mrl)
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'details' => 'Found unexpected request logger'
                ]);
        Assert::equal(count($robot->requestLoggers()), 1, 'request logger count');

        // Unregister request logger
        $robot->unregisterRequestLogger($mrl);
        Assert::equal(count($robot->requestLoggers()), 0, 'request logger count');
    }

            /*
             * Test content recorder management function
             */

    /**
     * Tests contentRecorder() and setContentRecorder() and also checks if
     * recorder records content correctly.
     */
    public function testSetAndGetContentRecorder()
    {
        $robot = $this->createRobot();

        // Set recorder
        $recorder = new ContentRecorder;
        $robot->setContentRecorder($recorder);
        if ($robot->contentRecorder() !== $recorder)
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'details' =>
                        'Expected content recorder found:' .
                        Error::formatValue($robot->contentRecorder())
                ]);

        // Check that content recorder records content correctly
        $content = 'When I am laid in earth';
        $identifier = $robot->recordContent($content, 'text/plain');
        Assert::equal(
            ContentRecorder::getContent($identifier),
            $content,
            'recorded content'
        );
    }

            /*
             * Test HTTP response access functions
             */

    /**
     * Tests reponse(). Makes a GET request and checks the response object.
     */
    public function testResponse()
    {
        $robot = $this->createRobot();

        // Make request
        $robot->get(self::mockUrl(['contentType' => 'text/html']));

        // Check response
        if (!$robot->response() instanceof \Psr\Http\Message\ResponseInterface)
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'details' =>
                        'Unexpected response: expected instance of ' .
                        'Psr\Http\Message\ResponseInterface; found:' .
                        Error::formatValue($robot->response())
                ]);
        Assert::equal($robot->response()->getStatusCode(), 200);
        Assert::equal($robot->response()->getReasonPhrase(), 'OK');
    }

    /**
     * Tests content() method
     */
    public function testContent()
    {
        $robot = $this->createRobot();
        $body = '<html><head><title>Content Test</title></head><body/></html>';
        $robot->get(self::mockUrl(['body' => $body]));
        Assert::equal(trim($robot->content()), $body);
    }

    /**
     * Tests hasMatch() method
     */
    public function testHasMatch1()
    {
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title></head><body/></html>';
        $robot->get(self::mockUrl(['body' => $body]));
        Assert::isTrue($robot->hasMatch('/hello/i'));
    }

    /**
     * Tests hasMatch() method
     */
    public function testHasMatch2()
    {
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title></head><body/></html>';
        $robot->get(self::mockUrl(['body' => $body]));
        Assert::isFalse($robot->hasMatch('/goodbye/'));
    }

    /**
     * Tests getMatch() method
     */
    public function testGetMatch()
    {
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title></head><body/></html>';
        $robot->get(self::mockUrl(['body' => $body]));
        Assert::equal($robot->getMatch('#<title>(HELLO)</title>#', 1), 'HELLO');
    }

    /**
     * Tests getAllMatches() method
     */
    public function testGetAllMatches()
    {
        $robot = $this->createRobot();
        $body = '<html><body><p>A</p><p>B</p></body></html>';
        $robot->get(self::mockUrl(['body' => $body]));
        $match =
            $robot->getAllMatches('#<p>([A-Z])</p>#', 1, PREG_PATTERN_ORDER);
        Assert::equal($match[1], ['A', 'B']);
    }

    /**
     * Tests contentType() method
     */
    public function testContentType()
    {
        $robot = $this->createRobot();
        foreach (['text/html', 'application/pdf', 'image/png'] as $expected) {
            $robot->get(self::mockUrl(['contentType' => $expected]));
            $found = $robot->contentType();
            $found = preg_replace('#(^[a-z]+/[a-z]+)\b.*#', '$1', $found);
            Assert::equal($found, $expected, 'unexpected content type');
        }
    }

    /**
     * Tests hasContentType() method
     */
    public function testHasContentType1()
    {
        $robot = $this->createRobot();
        $url = self::mockUrl(['contentType' => 'image/jpeg']);
        $robot->get($url);
        Assert::isTrue($robot->hasContentType('image/jpeg'));
    }

    /**
     * Tests hasContentType() method
     */
    public function testHasContentType2()
    {
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title><body/></html>';
        $url =
            self::mockUrl([
                'body' => $body,
                'contentType' => 'text/html'
            ]);
        $robot->get($url);
        Assert::isTrue($robot->hasContentType('text/html'));
    }

    /**
     * Tests hasContentType() method
     */
    public function testHasContentType3()
    {
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title><body/></html>';
        $url =
            self::mockUrl([
                'body' => $body,
                'contentType' => 'text/html;charset=utf-8'
            ]);
        $robot->get($url);
        Assert::isTrue($robot->hasContentType('text/html'));
    }

    /**
     * Tests hasContentType() method
     */
    public function testHasContentType4()
    {
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title><body/></html>';
        $url =
            self::mockUrl([
                'body' => $body,
                'contentType' => 'text/html; charset=utf-8'
            ]);
        $robot->get($url);
        Assert::isTrue($robot->hasContentType('text/html'));
    }

    /**
     * Tests hasContentType() method
     */
    public function testHasContentType5()
    {
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title><body/></html>';
        $url =
            self::mockUrl([
                'body' => $body,
                'contentType' => 'text/html ;charset=utf-8'
            ]);
        $robot->get($url);
        Assert::isTrue($robot->hasContentType('text/html'));
    }

    /**
     * Tests assertContentType() method
     */
    public function testAssertContentType1()
    {
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title><body/></html>';
        $url =
            self::mockUrl([
                'body' => $body,
                'contentType' => 'text/html; charset=utf-8'
            ]);
        $robot->get($url);
        $robot->assertContentType('text/html');
        $robot->get($url, [
            'test' =>
                function($resp) use ($robot)
                {
                    $robot->assertContentType('text/html');
                }
        ]);
    }

    /**
     * Tests assertContentType() method
     */
    public function testAssertContentType2()
    {
        $this->setExpectedStatusCode('UNEXPECTED_BEHAVIOR');
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title><body/></html>';
        $url =
            self::mockUrl([
                'body' => $body,
                'contentType' => 'text/html; charset=utf-8'
            ]);
        $robot->get($url);
        $robot->assertContentType('text/xml');
    }

    /**
     * Tests assertContentType() method
     */
    public function testAssertContentType3()
    {
        $this->setExpectedStatusCode('UNEXPECTED_BEHAVIOR');
        $robot = $this->createRobot();
        $body = '<html><head><title>HELLO</title><body/></html>';
        $url =
            self::mockUrl([
                'body' => $body,
                'contentType' => 'text/html; charset=utf-8'
            ]);
        $robot->get($url, [
            'test' =>
                function($resp) use ($robot)
                {
                    $robot->assertContentType('text/xml');
                }
        ]);
    }

            /*
             * Test cookie methods
             */

    /**
     * Tests cookies() and setCookie() functions with all possible options
     */
    public function testSetAndGetCookies()
    {
        $robot = $this->createRobot();
        $cookie =
            [
                'name' => 'yummy_cookie',
                'value' => 'choco',
                'expires' => "0",
                'path' => '/',
                'domain' => 'yahoo.com',
                'secure' => true,
                'httpOnly' => false
            ];
        $robot->setCookie($cookie);
        Assert::equal(
            $robot->cookies(),
            [$cookie],
            'cookie'
        );
    }

   /**
    * Tests if cookies has been sent by robot to the server. This test creates
    * a GET request to ECHO_FORM_DATA after setting a cookie and then check
    * if response contains cookie element.
    */
    public function testSendRequestWithCookies()
    {
        $robot = $this->createRobot();

        // Set cookies
        $cookies =
            [
                'animal' => 'she-goat',
                'vegetable' => 'leek'
            ];
        foreach ($cookies as $n => $v) {
            $name = 'animal';
            $value = 'she-goat';
            $config = \CodeRage\Config::current();
            $cookie =
                [
                    'name' => $n,
                    'value' => $v,
                    'expires' => Time::real() + 100000,
                    'path' => '/',
                    'secure' => true,
                    'domain' => self::HTTP_ECHO_DOMAIN
                ];
            $robot->setCookie($cookie);
        }

        // Send request
        $robot->get(self::httpechoUrl('cookies'));
        $response = $this->decodeHttpEchoResponse($robot);
        var_dump($response);

        // Check if response contains cookies array
        if (!isset($response['cookies'])) {
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'details' => 'Missing cookies'
                ]);
        }
        Assert::equal((array) $response['cookies'], $cookies);
    }

    /**
     * Tests if cookies has been recieved by robot sent by the server. This test
     * creates a GET request to MOCK_RESPONSE with "cookies" query parameter and
     * then check if cookie sent in response is set in cookie jar
     */
    public function testRecieveResponseWithCookies()
    {
        $robot = $this->createRobot();
        $name = 'yummy_cookie';
        $value = 'choco';

        // Send request to MOCK_RESPONSE with 'cookies' parameter. The response
        // of the request will contain a cookie
        $robot->get(
            self::mockUrl([
                'body' => 'Hello, World!',
                'cookies[0][name]' => $name,
                'cookies[0][value]' => $value
            ])
        );

        // Check if cookie return by MOCK_RESPONSE is in cookie jar
        $cookies = $robot->cookies();
        if (!isset($cookies[0]))
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'details' => "Missing cookie"
                ]);

        // Check if cookie have expected name and value
        if ($cookies[0]['name'] != $name || $cookies[0]['value'] != $value)
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'details' =>
                        "Expected cookie with name: '$name' and value: $value; " .
                        "found cookie with name: '{$cookies[0]['value']}' " .
                        "and value: '{$cookies[0]['value']}'"
                ]);
    }

    /**
     * Tests setCookie() function with invalid "name" option
     */
    public function testSetCookieWithInvalidNameFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 123,
                'value' => 'choco',
                'domain' => 'xxx.com',
            ]);
    }

    /**
     * Tests setCookie() function with invalid "value" option
     */
    public function testSetCookieWithInvalidValueFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 'yummy_cookie',
                'value' => 123,
                'path' => '/',
                'domain' => 'xxx.com',
            ]);
    }

    /**
     * Tests setCookie() function with invalid "expires" option
     */
    public function testSetCookieWithInvalidExpiresFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 'yummy_cookie',
                'value' => 'choco',
                'expires' => [],
                'path' => '/',
                'domain' => 'xxx.com',
            ]);
    }

    /**
     * Tests setCookie() function with invalid "path" option
     */
    public function testSetCookieWithInvalidPathFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 'yummy_cookie',
                'value' => 'choco',
                'expires' => 100000,
                'path' => [],
                'domain' => 'xxx.com',
            ]);
    }

    /**
     * Tests setCookie() function with invalid "domain" option
     */
    public function testSetCookieWithInvalidDomainFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 'yummy_cookie',
                'value' => 'choco',
                'expires' => 100000,
                'path' => '/',
                'domain' => []
            ]);
    }

    /**
     * Tests setCookie() function with invalid "secure" option
     */
    public function testSetCookieWithInvalidSecureFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 'yummy_cookie',
                'value' => 'choco',
                'expires' => 100000,
                'path' => '/',
                'domain' => 'xxx.com',
                'secure' => []
            ]);
    }

    /**
     * Tests setCookie() function with invalid "httpOnly" option
     */
    public function testSetCookieWithInvalidHttpOnlyFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 'yummy_cookie',
                'value' => 'choco',
                'expires' => 100000,
                'path' => '/',
                'domain' => 'xxx.com',
                'secure' => true,
                'httpOnly' => []
            ]);
    }

    /**
     * Tests setCookie() function with wissing "name" option
     */
    public function testSetCookieWithMissingNameFailure()
    {
        $this->setExpectedStatusCode('MISSING_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'value' => 'choco',
                'domain' => 'xxx.com',
                'secure' => true
            ]);
    }

    /**
     * Tests setCookie() function with wissing "value" option
     */
    public function testSetCookieWithMissingValueFailure()
    {
        $this->setExpectedStatusCode('MISSING_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 'yummy_cookie',
                'domain' => 'xxx.com',
                'secure' => true
            ]);
    }

    /**
     * Tests setCookie() function with wissing "domain" option
     */
    public function testSetCookieWithMissingDomainFailure()
    {
        $this->setExpectedStatusCode('MISSING_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 'yummy_cookie',
                'value' => 'choco',
                'secure' => true
            ]);
    }

    /**
     * Tests setCookie() function with wissing "secure" option
     */
    public function testSetCookieWithMissingSecureFailure()
    {
        $this->setExpectedStatusCode('MISSING_PARAMETER');
        $robot = $this->createRobot();
        $robot->setCookie(
            [
                'name' => 'yummy_cookie',
                'value' => 'choco',
                'domain' => 'xxx.com'
            ]);
    }

            /*
             * Test setForm() function with various options
             */

    /**
     * Tests setForm() function with "name" option. This method uses
     * RobotSuite::checkSetForm() helper method to run following sequence of
     * operations:
     *   - Make request to MOCK_RESPONSE that generate HTML with 2 forms
     *   - It selects the second form using form name and send the request and
     *     validate the response.
     *   - Use CodeRage\Tool\Robots back() to load HTML with forms again
     *   - It selects the first form using form name and send the request and
     *     validate the response.
     */
    public function testSetFormUsingName()
    {
        $this->checkSetForm([
            'form1' => ['name' => 'testform1'],
            'form2' => ['name' => 'testform2']
        ]);
    }

    /**
     * Tests setForm() function with "id" option. This method uses
     * RobotSuite::checkSetForm() helper method to run following sequence of
     * operations:
     *   - Make request to MOCK_RESPONSE that generate HTML with 2 forms
     *   - It selects the second form using form id and send the request and
     *     validate the response.
     *   - Use CodeRage\Tool\Robots back() to load HTML with forms again
     *   - It selects the first form using form id and send the request and
     *     validate the response.
     */
    public function testSetFormUsingId()
    {
        $this->checkSetForm([
            'form1' => ['id' => 'testform1_id'],
            'form2' => ['id' => 'testform2_id']
        ]);
    }

    /**
     * Tests setForm() function with "selector" option. This method uses
     * RobotSuite::checkSetForm() helper method to run following sequence of
     * operations:
     *   - Make request to MOCK_RESPONSE that generate HTML with 2 forms
     *   - It selects the second form using form selector and send the request
     *     and validate the response.
     *   - Use CodeRage\Tool\Robots back() to load HTML with forms again
     *   - It selects the first form using form selector and send the request
     *     and validate the response.
     */
    public function testSetFormUsingSelector()
    {
        $this->checkSetForm([
            'form1' => ['selector' => '#testform1_id'],
            'form2' => ['selector' => '#testform2_id']
        ]);
    }

    /**
     * Tests setForm() function with "xpath" option. This method uses
     * RobotSuite::checkSetForm() helper method to run following sequence of
     * operations:
     *   - Make request to MOCK_RESPONSE that generate HTML with 2 forms
     *   - It selects the second form using xpath expression of second form and
     *     send the request and validate the response.
     *   - Use CodeRage\Tool\Robots back() to load HTML with forms again
     *   - It selects the first form using xpath expression of first form and
     *     send the request and validate the response.
     */
    public function testSetFormUsingXpath()
    {
        $this->checkSetForm([
            'form1' => ['xpath' => "html/body/form[@name='testform1']"],
            'form2' => ['xpath' => "html/body/form[@id='testform2_id']"]
        ]);
    }

    /**
     * Tests setForm() function with invalid "name" option
     */
    public function testSetFormWithInvalidNameOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setForm(['name' => 0]);
    }

    /**
     * Tests setForm() function with invalid "id" option
     */
    public function testSetFormWithInvalidIdOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setForm(['id' => 0]);
    }

    /**
     * Tests setForm() function with invalid "selector" option
     */
    public function testSetFormWithInvalidSelectorOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setForm(['selector' => 0]);
    }

    /**
     * Tests setForm() function with invalid "xpath" option
     */
    public function testSetFormWithInvalidXpathOptionFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();
        $robot->setForm(['xpath' => 0]);
    }

    /**
     * Tests setForm() function with inconsistent options
     */
    public function testSetFormWithInconsistentOptionsFailure()
    {
        $this->setExpectedStatusCode('INCONSISTENT_PARAMETERS');
        $robot = $this->createRobot();

        // Option "name" and "id" cannot be set together
        $robot->setForm(['name' => 'form1', 'id' => 'from1id']);
    }

    /**
     * Tests setForm() function with form name that does not exists in HTML page
     */
    public function testSetFormWithNonExistingFormNameFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_CONTENT');
        $robot = $this->createRobot();

        // This function is expected to throw an error when setting a
        // form as no form with name 'xxx' exists in the HTML page used
        // by RobotSuite::checkSetForm() when setting form
        $this->checkSetForm([
            'form1' => ['name' => 'xxx'],
            'form2' => ['name' => 'xxx']
        ]);
    }

    /**
     * Tests setForm() function with HTML page that does not have form
     */
    public function testSetFormWithNonExistingForm()
    {
        $this->setExpectedStatusCode('UNEXPECTED_CONTENT');
        $robot = $this->createRobot();

        // Make a request to MOCK_RESPONSE that does not load any form
        $robot->get(self::mockUrl(['contentType' => 'text/html']));

        // This function is expected to throw an error because HTML response
        // does not contain any form
        $robot->setForm(['name' => 'xxx']);
    }

            /*
             * Test setFields() function with various options
             */

    /**
     * Tests setFields() function for input type text. This function uses
     * RobotSuite::checkSetFields() to run following sequence of operations:
     *   - Make request to MOCK_RESPONSE that generate a HTML forms contains
     *     input type "text"
     *   - Sets values to input fields of type "text"
     *   - Assert the response
     */
    public function testSetFieldsWithInputTypeText()
    {
        $robot = $this->createRobot();
        $this->checkSetFields([
            'inputs' =>
                [
                    [
                       'type' => 'text',
                       'name' => 'name'
                    ]
                ],
            'values' => ['name' => 'carl'],
            'response' =>
                [
                    'method' => 'POST',
                    'statusCode' => 200,
                    'reasonPhrase' => 'OK',
                    'contentType' => 'application/x-www-form-urlencoded',
                    'form' => [
                        [
                            'name' => 'name',
                            'value' => 'carl'
                        ]
                    ]
                ]
        ]);
    }

    /**
     * Tests setFields() function for input type select. This function uses
     * RobotSuite::checkSetFields() to run following sequence of operations:
     *   - Make request to MOCK_RESPONSE that generate a HTML forms contains
     *     input type "select"
     *   - Sets values to input fields of type "select"
     *   - Assert the response
     */
    public function testSetFieldsWithInputTypeSelect()
    {
        $this->checkSetFields([
            'inputs' =>
                [
                    [
                        'type' => 'select',
                        'name' => 'color',
                        'options' =>
                            [
                                [
                                    'value' => 'yellow',
                                    'label' => 'yellow',
                                ],
                                [
                                    'value' => 'red',
                                    'label' => 'red',
                                ]
                            ]
                    ],
                ],
            'values' => [ 'color' => 'red' ],
            'response' =>
                [
                    'method' => 'POST',
                    'statusCode' => 200,
                    'reasonPhrase' => 'OK',
                    'contentType' => 'application/x-www-form-urlencoded',
                    'form' =>
                        [
                            [
                                'name' => 'color',
                                'value' => 'red'
                            ]
                        ]
                ]
        ]);
    }

    /**
     * Tests setFields() function for input type multi-select. This function
     * uses RobotSuite::checkSetFields() to run following sequence of operations:
     *   - Make request to MOCK_RESPONSE that generate a HTML forms contains
     *     input type "select" that allow multiple selections
     *   - Sets values to input fields of type "select"
     *   - Assert the response
     */
    public function testSetFieldsWithInputTypeMultiSelect()
    {
        $this->checkSetFields([
            'inputs' =>
                [
                    [
                        'type' => 'select',
                        'name' => 'colors',
                        'multiple' => true,
                        'options' =>
                            [
                                [
                                    'value' => 'yellow',
                                    'label' => 'yellow',
                                ],
                                [
                                    'value' => 'red',
                                    'label' => 'red',
                                ]
                            ]
                    ],
                ],
            'values' => [ 'colors' => ['red', 'yellow'] ],
            'response' =>
                [
                    'method' => 'POST',
                    'statusCode' => 200,
                    'reasonPhrase' => 'OK',
                    'contentType' => 'application/x-www-form-urlencoded',
                    'form' =>
                        [
                            [
                                'name' => 'colors[0]',
                                'value' => 'red'
                            ],
                            [
                                'name' => 'colors[1]',
                                'value' => 'yellow'
                            ]
                        ]
                ]
        ]);
    }

    /**
     * Tests setFields() function for input type file. This function uses
     * RobotSuite::checkSetFields() to run following sequence of operations:
     *   - Make request to MOCK_RESPONSE that generate a HTML forms contains
     *     input type "file"
     *   - Sets values to input fields of type "file"
     *   - Assert the response
     */
    public function testSetFieldsWithInputTypeFile()
    {
        $temp = File::temp();
        file_put_contents($temp, 'Test content');
        $this->checkSetFields([
            'inputs' =>
                [
                    [
                        'type' => 'file',
                        'name' => 'file'
                    ]
                ],
            'values' =>
                [
                    'file' =>
                        [
                            'path' => $temp,
                            'filename' => 'testfile.txt',
                            'contentType' => 'text/plain'
                        ]
                ],
            'response' =>
                [
                    'method' => 'POST',
                    'statusCode' => 200,
                    'reasonPhrase' => 'OK',
                    'contentType' => 'multipart/form-data',
                    'form' =>
                        [
                            [
                                'name' => 'file',
                                'contentType' => 'text/plain',
                                'filename' => 'testfile.txt',
                                'value' => file_get_contents($temp)
                            ]
                        ]
                ]
        ]);
    }

    /**
     * Tests setFields() function with field name that does not exists in form
     */
    public function testSetFieldsWithNonExistingFormFieldFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_CONTENT');
        $robot = $this->createRobot();

        // Load form with only input of type text with name "name"
        $this->loadMockPage($robot, self::formPageWithInputTypeText());

        // This function is expected to fail because loaded HTML form does not
        // contain any field with name 'xxx'
        $robot->setFields(['xxx' => 'xxx']);
    }

    /**
     * Tests setFields() function by setting array value for field of input type
     * "text"
     */
    public function testSetFieldsWithArrayValueForWrongInputTypeFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_CONTENT');
        $robot = $this->createRobot();

        // Load form with only input of type text with name "name"
        $this->loadMockPage($robot, self::formPageWithInputTypeText());

        // This function is expected to fail as input of type "text" cannot
        // accept array values
        $robot->setFields(['name' => ['xxx', 'yyy']]);
    }

    /**
     * Tests setFields() function by setting invalid "path" option of input type
     * file
     */
    public function testSetFieldsWithInvalidPathOptionOfFileInputFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();

        // Load form with only input of type file with name "file"
        $this->loadMockPage($robot, self::formPageWithInputTypeFile());

        // This function is expected to fail as "path" option is invalid
        $robot->setFields(
            [
                'file' =>
                    [
                        'path' => 0,
                        'filename' => 'file.txt',
                        'contentType' => 'text/plain'
                    ]
            ]
        );
    }

    /**
     * Tests setFields() function by setting invalid "filename" option of input
     * type file
     */
    public function testSetFieldsWithInvalidFilenameOptionOfFileInputFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();

        // Load form with only input of type file with name "file"
        $this->loadMockPage($robot, self::formPageWithInputTypeFile());

        // This function is expected to fail as "filename" option is invalid
        $robot->setFields(
            [
                'file' =>
                    [
                        'path' => File::temp(),
                        'filename' => 0,
                        'contentType' => 'text/plain'
                    ]
            ]
        );
    }

    /**
     * Tests setFields() function by setting invalid "contentType" option of
     * input type file
     */
    public function testSetFieldsWithInvalidContentTypeOptionOfFileInputFailure()
    {
        $this->setExpectedStatusCode('INVALID_PARAMETER');
        $robot = $this->createRobot();

        // Load form with only input of type file with name "file"
        $this->loadMockPage($robot, self::formPageWithInputTypeFile());

        // This function is expected to fail as "contentType" option is invalid
        $robot->setFields(
            [
                'file' =>
                    [
                        'path' => File::temp(),
                        'filename' => 'file.txt',
                        'contentType' => 0
                    ]
            ]
        );
    }

            /*
             * Test setFileUploadField() function
             */

    /**
     * Tests setFileUploadField() function by setting non existing $path
     * parameter
     */
    public function testSetFileUploadFieldWithNonExistingFilePathFailure()
    {
        $this->setExpectedStatusCode('OBJECT_DOES_NOT_EXIST');
        $robot = $this->createRobot();

        // Load form with only input of type file with name "file"
        $this->loadMockPage($robot, self::formPageWithInputTypeFile());

        // This function is expected to fail as file path does not exists
        $robot->setFileUploadField(
            'file', '/path/does/not/exists', 'test.txt', 'text/plain'
        );
    }

    /**
     * Tests setFileUploadField() function with $name parameter that does not
     * exists in form
     */
    public function testSetFileUploadFieldWithNonExistingFormFieldFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_CONTENT');
        $robot = $this->createRobot();

        // Load form with only input of type text with name "name"
        $this->loadMockPage($robot, self::formPageWithInputTypeText());

        // This function is expected to fail as loaded HTML form does not
        // contain any field with name "file"
        $robot->setFileUploadField(
            'file', File::temp(), 'test.txt', 'text/plain'
        );
    }

    /**
     * Tests setFileUploadField() function with field name that is not of type
     * "file"
     */
    public function testSetFileUploadFieldWithFieldWithWrongInputTypeFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_CONTENT');
        $robot = $this->createRobot();

        // Load form with only input of type text with name "name"
        $this->loadMockPage($robot, self::formPageWithInputTypeText());

        // This function is expected to fail as loaded HTML form contains
        // input type of text with name "name"
        $robot->setFileUploadField(
            'name', File::temp(), 'test.txt', 'text/plain'
        );
    }

             /*
              * Test get() function
              */

    /**
     * Tests get() function with a regular expression test
     */
    public function testGetWithRegexTest()
    {
        $robot = $this->createRobot();
        $url =
            self::mockUrl([
                'body' => '<html><head><title>HELLO</title><body/></html>',
                'contentType' => 'text/html; charset=utf-8'
            ]);
        $robot->get($url, ['test' => '/<title>HELLO<\/title>/']);
    }

    /**
     * Tests get() function with a regular expression test
     */
    public function testGetWithRegexTestAndErrorMessageFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_CONTENT');
        $robot = $this->createRobot();
        $url =
            self::mockUrl([
                'body' => '<html><head><title>HELLO</title><body/></html>',
                'contentType' => 'text/html; charset=utf-8'
            ]);
        $errorMsg = 'Get request failed';
        try {
            $robot->get(
                $url,
                [
                    'errorMessage' => $errorMsg,
                    'test' => '/<title>GOODBYE<\/title>/'
                ]
            );
        } catch (Throwable $e) {
            if (strpos($e->getMessage(), $errorMsg) === false)
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' =>
                            "Expected error message '$errorMsg ...' found: $e"
                    ]);
            throw $e;
        }
    }

    /**
     * Tests get() function with a custom status code test
     */
    public function testGetWithCustomStatusTest()
    {
        $robot = $this->createRobot();
        $url = self::mockUrl(['statusCode' => 301]);
        $robot->get($url, ['test' => 301]);
    }

    /**
     * Tests get() function with a custom status code test
     */
    public function testGetWithCustomStatusAndErrorMessageFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_BEHAVIOR');
        $robot = $this->createRobot();
        $url = self::mockUrl(['statusCode' => 200]);
        $errorMsg = 'Get request failed';
        try {
            $robot->get($url, [
                'errorMessage' => $errorMsg,
                'test' => 302
            ]);
        } catch (Throwable $e) {
            if (strpos($e->getMessage(), $errorMsg) === false)
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' =>
                            "Expected error message '$errorMsg ...' found: $e"
                    ]);
            throw $e;
        }
    }

    /**
     * Tests get() function with a callable test
     */
    public function testGetWithCallableTest()
    {
        $robot = $this->createRobot();
        $url =
            self::mockUrl([
                'contentType' => 'text/html; charset=utf-8'
            ]);
        $robot->get($url, [
            'test' =>
                function($resp)
                {
                    $ct = $resp->getHeader('Content-Type')[0] ?? '';
                    if (!preg_match('/UTF-8/i', $ct))
                        throw new
                            Error([
                                'status' => 'ASSERTION_FAILED',
                                'details' => "Expected UTF-8; found '$ct'"
                            ]);
                }
        ]);
    }

    /**
     * Tests get() function with a callable test
     */
    public function testGetWithCallableFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_BEHAVIOR');
        $robot = $this->createRobot();
        $url =
            self::mockUrl([
                'contentType' => 'text/html; charset=utf-8'
            ]);
        $robot->get(
            $url, [
                'test' =>
                    function($resp)
                    {
                        $ct = $resp->getHeader('Content-Type')[0] ?? '';
                        if (!preg_match('/UTF-16/i', $ct))
                            throw new
                                Error([
                                    'status' => 'UNEXPECTED_BEHAVIOR',
                                    'details' =>
                                        "Expected UTF-16; found '$ct'"
                                ]);
                    }
        ]);
    }

    /**
     * Tests get() function with "headers" option. This function checks if
     * Robot sets the headers when making a GET request with "headers" option.
     * This function uses RobotSuite::checkGet() to run following sequence of
     * operations:
     *   - Make a GET request to ECHO_FORM_DATA with given headers
     *   - Assert the response
     */
    public function testGetWithHeadersOption()
    {
        $robot = $this->createRobot();
        $headers = ['TestHeader' => 'TestValue'];
        $this->checkGet(
            $robot,
            ['headers' => $headers], // Options to Robot::get()
            [
                'method' => 'GET',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => '',
                'headers' => $headers,
                'form' => []
            ]
        );
    }

    /**
     * Tests get() function with "outputFile" option. This function checks if
     * Robot stores the result to a file set by "outputFile" option
     */
    public function testGetWithOutputFileOption()
    {
        $robot = $this->createRobot();
        $temp = File::temp();
        $robot->get(
            self::mockUrl(['body' => 'TestBody']),
            [ 'outputFile' => $temp ]
        );

        // Check if file set in "outputFile" option contains the response
        Assert::equal(trim(file_get_contents($temp)), 'TestBody');
    }

    /**
     * Tests get() function after setting multiple request loggers
     */
    public function testGetWithRequestLoggers()
    {
        if ($this->skipLongCases()) {
            return;
        }

        $robot = $this->createRobot();
        $log = [];

        // Define sequence of callables of preRequest() and postRequest() for
        // MockRequestLogger.
        // 1st preRequest() throw error and 1st postRequest() method doesn't
        // 2nd preRequest() doesn't throw error but 2nd postRequest() method does
        // 3rd preRequest() and postRequest() does not throw error
        $rl =
            new \CodeRage\Tool\Test\MockRequestLogger(
                [
                    function($robot, $method, $uri) use(&$log)
                    {
                        $log[] = 'A';
                        throw new
                            Error([
                                'status' => 'RETRY',
                                'message' => 'Request failed'
                            ]);
                    },
                    function($robot, $method, $uri) use(&$log)
                    {
                        $log[] = 'B';
                    },
                    function($robot, $method, $uri) use(&$log)
                    {
                        $log[] = 'C';
                    }
                ],
                [
                    function($robot, $method, $uri, $error) use(&$log)
                    {
                        $log[] = $error === null ? 'D1' : 'D2';
                    },
                    function($robot, $method, $uri, $error) use(&$log)
                    {
                        $log[] = $error === null ? 'E1' : 'E2';
                        throw new
                            Error([
                                'status' => 'RETRY',
                                'message' => 'Request failed'
                            ]);
                    },
                    function($robot, $method, $uri, $error) use(&$log)
                    {
                        $log[] = $error === null ? 'F1' : 'F2';
                    }
                ]
            );
        $robot->registerRequestLogger($rl);
        $robot->get(self::httpechoUrl());

        // Assert the sequence of calls to pre and post requests.
        // When operation is run for the first time it calls following sequence
        // of pre and post request methods:
        //   - 1st call to preRequest() method is made which logs the message
        //     'A' and throws an error with status 'RETRY'
        //   - 1st call to postRequest() method is made from error handle which
        //     logs the message 'D2'
        // In error handle as error status is 'RETRY', operation will be
        // repeat for first time and calls following sequence of of pre and
        // post request methods:
        //   - 2nd call to preRequest() method is made which logs a message
        //     'B'
        //   - 2nd call to postRequest() method is made which logs a message
        //     'E1' and throws an error with status 'RETRY'
        //   - 3rd call to postRequest() method is made from error handle which
        //     logs a message 'F2'
        // In error handle as error status is 'RETRY', operation will be
        // repeat for second time and calls following sequence of pre and post
        // request methods:
        //   - 3rd call to preRequest() method is made which logs a mesage
        //     'C'
        //   - 4th call to postRequest() method is made which logs a mesage
        //     'F1' (3rd postRequest() method in array is called again as there
        //     is no 4th postRequest method)
        Assert::equal($log, ['A', 'D2', 'B', 'E1', 'F2', 'C', 'F1']);
    }

    /**
     * Tests get() request with "accept", "user-agent" and "accept-language"
     * headers. This function checks if headers are set in request sent by
     * Robot. This function uses RobotSuite::checkGet() to run following
     * sequence of operations:
     *   - Make a GET request to ECHO_FORM_DATA
     *   - Assert the response
     */
    public function testGetWithAcceptUserAgentAndAcceptLanguageHeaders()
    {
        // Set headers in constructor
        $robot =
            new BasicRobot([
                    'userAgent' => 'Safari/537.36',
                    'accept' => 'application/json',
                    'acceptLanguage' => 'en-us'
                ]);

        // Make GET request
        $this->checkGet(
            $robot,
            [ ], // empty options to Robot::get()
            [
                'method' => 'GET',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'headers' =>
                    [
                        'user-agent' => 'Safari/537.36',
                        'accept' => 'application/json',
                        'accept-language' => 'en-us'
                    ],
                'contentType' => '',
                'form' => []
            ]
        );
    }

            /*
             * Test post() function
             */

    /**
     * Tests post() function with a regular expression test
     */
    public function testPostWithRegexTest()
    {
        $robot = $this->createRobot();
        $slogan = 'Fly the friendly skies';;
        $robot->post(self::httpEchoUrl(), [
            'test' => self::dataUriRegex($slogan),
            'headers' => ['Content-Type' => 'application/x-www-form-urlencoded'],
            'postData' => ['slogan' => $slogan]
        ]);
    }

    /**
     * Tests post() function with a regular expression test
     */
    public function testPostWithRegexTestAndErrorMessageFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_CONTENT');
        $errorMsg = 'Post request failed';
        try {
            $this->createRobot()->post(self::httpechoUrl(), [
                'errorMessage' => $errorMsg,
                'test' => '/Keep Climbing/',
                'postData' => ['slogan' => 'Fly the friendly skies']
            ]);
        } catch (Throwable $e) {
            if (strpos($e->getMessage(), $errorMsg) === false)
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' =>
                            "Expected error message '$errorMsg ...' found: $e"
                    ]);
            throw $e;
        }
    }

    /**
     * Tests post() function with a custom status code test
     */
    public function testPostWithCustomStatusTest()
    {
        $config = Config::current();
        $url =
            ($config->getProperty('ssl', 0) ? 'https://' : 'http://') .
            $config->getProperty('site_domain');
        if ($config->hasProperty('site_port'))
            $url .= ':' . $config->getProperty('site_port');
        $url .= '/does/not/exist';
        $this->createRobot()->post($url, ['test' => 404]);
    }

    /**
     * Tests post() function with a custom status code test
     */
    public function testPostWithCustomStatusAndErrorMessageFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_BEHAVIOR');
        $errorMsg = 'Post request failed';
        try {
            $this->createRobot()->post(self::httpechoUrl(), [
                'errorMessage' => $errorMsg,
                'test' => 404
            ]);
        } catch (Throwable $e) {
            if (strpos($e->getMessage(), $errorMsg) === false)
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' =>
                            "Expected error message '$errorMsg ...' found: $e"
                    ]);
            throw $e;
        }
    }

    /**
     * Tests post() function with a callable test
     */
    public function testPostWithCallableTest()
    {
        $this->createRobot()->post(self::httpechoUrl(), [
            'test' =>
                function($resp)
                {
                    $ct = $resp->getHeader('Content-Type')[0] ?? '';
                    if ($ct != 'application/json')
                        throw new
                            Error([
                                'status' => 'UNEXPECTED_BEHAVIOR',
                                'details' =>
                                    "Expected 'application/json'; found '$ct'"
                            ]);
                }
        ]);
    }

    /**
     * Tests post() function with a custom status code test
     */
    public function testPostWithCallableAndErrorMessageFailure()
    {
        $this->setExpectedStatusCode('UNEXPECTED_BEHAVIOR');
        $this->createRobot()->post(self::httpechoUrl(), [
            'test' =>
                function($resp)
                {
                    $ct = $resp->getHeader('Content-Type')[0] ?? '';
                    if ($ct != 'text/html')
                        throw new
                            Error([
                                'status' => 'UNEXPECTED_BEHAVIOR',
                                'details' =>
                                    "Expected 'text/html'; found '$ct'"
                            ]);
                }
        ]);
    }

    /**
     * Tests post() function with "headers" option. This function checks if
     * Robot sets the headers when making a POST request with "headers" option.
     * This function uses RobotSuite::checkPost() to run following sequence of
     * operations:
     *   - Make a POST request to ECHO_FORM_DATA with post data containing
     *     headers
     *   - Assert the response
     */
    public function testPostWithHeadersOption()
    {
        $headers = ['TestHeader' => 'TestValue'];

        // Make a POST request with "headers" option
        $this->checkPost(
            $this->createRobot(),
            [ 'headers' => $headers ], // Options to Robot::post()
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'headers' => $headers,
                'contentType' => 'application/x-www-form-urlencoded',
                'form' => []
            ]
        );
    }

    /**
     * Tests post() function with "outputFile" option. This function checks if
     * Robot stores the result to a file set by "outputFile" option.
     * This function uses RobotSuite::checkPost() to run following sequence of
     * operations:
     *   - Make a POST request to ECHO_FORM_DATA with post data and "outputFile"
     *     option
     *   - Assert the response
     */
    public function testPostWithOutputFileOption()
    {
        $temp = File::temp();
        $this->checkPost(
            $this->createRobot(),
            [
                'postData' => ['body' => 'This is test string'],
                'outputFile' => $temp
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'body',
                            'value' => 'This is test string'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests post() function with postData with fields having scalar values.
     * This function uses RobotSuite::checkPost() to run following sequence of
     * operations:
     *   - Make a POST request to ECHO_FORM_DATA with post data field names
     *     mapping to string values
     *   - Assert the response
     */
    public function testPostWithPostDataWithStringValues()
    {
        $robot = $this->createRobot();
        $this->checkPost(
            $robot,
            [
	           'postData' =>
                    [
                        'name' => 'carl',
                        'age' => '18',
                    ]
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' => [
                    (object)[
                        'name' => 'name',
                        'value' => 'carl'
                    ],
                    (object)[
                        'name' => 'age',
                        'value' => '18'
                    ]
                ]
            ]
        );
    }

    /**
     * Tests post() function with postData with non scalar fields values. This
     * function uses RobotSuite::checkPost() to run following sequence of
     * operations:
     *   - Make a POST request to ECHO_FORM_DATA with post data field names
     *     mapping to array or an associative array
     *   - Assert the response
     */
    public function testPostWithPostDataWithFileUpload()
    {
        $temp = File::temp();
        $contents =
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed \n" .
            "do eiusmod tempor incididunt ut labore et dolore magna \n" .
            "aliqua. Ut enim ad minim veniam, quis nostrud exercitation \n" .
            "ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis \n" .
            "aute irure dolor in reprehenderit in voluptate velit esse \n" .
            "cillum dolore eu fugiat nulla pariatur. Excepteur sint \n" .
            "occaecat cupidatat non proident, sunt in culpa qui officia \n" .
            "deserunt mollit anim id est laborum\n";
        file_put_contents($temp, $contents);
        $this->checkPost(
            $this->createRobot(),
            [
	           'postData' =>
                    [
                        'firstName' => 'Serge',
                        'lastName' => 'Koussevitzky',
                        'wife' =>
                            ['Nadezhda Galat', 'Natalie Ushkova', 'Olga Naumova'],
                        'file' =>
                            [
                                'path' => $temp,
                                'filename' => 'lorem-ipsum.txt',
                                'contentType' => 'text/plain'
                            ]
                    ]
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'multipart/form-data',
                'form' => [
                    [
                        'name' => 'firstName',
                        'value' => 'Serge'
                    ],
                    [
                        'name' => 'lastName',
                        'value' => 'Koussevitzky'
                    ],
                    [
                        'name' => 'wife',
                        'value' => 'Nadezhda Galat'
                    ],
                    [
                        'name' => 'wife',
                        'value' => 'Natalie Ushkova'
                    ],
                    [
                        'name' => 'wife',
                        'value' => 'Olga Naumova'
                    ],
                    [
                        'name' => 'file',
                        'value' => $contents,
                        'contentType' => 'text/plain' ,
                        'filename' => 'lorem-ipsum.txt'
                    ]
                ]
            ]
        );
    }

    /**
     * Tests post() function with postData with non scalar fields values. This
     * function uses RobotSuite::checkPost() to run following sequence of
     * operations:
     *   - Make a POST request to ECHO_FORM_DATA with post data field names
     *     mapping to array or an associative array
     *   - Assert the response
     */
    public function testPostWithEnctypeMultipartFormData()
    {
        $this->checkPost(
            $this->createRobot(),
            [
	           'postData' =>
                    [
                        'firstName' => 'Serge',
                        'lastName' => 'Koussevitzky',
                        'wife' =>
                            ['Nadezhda Galat', 'Natalie Ushkova', 'Olga Naumova']
                    ],
                'multipart' => true
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'multipart/form-data',
                'form' =>
                    [
                        [
                            'name' => 'firstName',
                            'value' => 'Serge'
                        ],
                        [
                            'name' => 'lastName',
                            'value' => 'Koussevitzky'
                        ],
                        [
                            'name' => 'wife',
                            'value' => 'Nadezhda Galat'
                        ],
                        [
                            'name' => 'wife',
                            'value' => 'Natalie Ushkova'
                        ],
                        [
                            'name' => 'wife',
                            'value' => 'Olga Naumova'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests a raw POST request
     */
    public function testPostRaw()
    {
        $body =
            "Was he free? Was he happy? The question is absurd:\n" .
            "Had anything been wrong, we should certainly have heard.";
        $this->checkPost(
            $this->createRobot(),
            [
	            'body' => $body,
                'headers' => ['Content-Type' => 'text/plain']
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'text/plain',
                'body' => $body
            ]
        );
    }

            /*
             * Test submit() function
             */

    /**
     * Tests submit() function with "errorMessage" and "test" option
     */
    public function testSubmitWithErrorMessageAndTestOption()
    {
        $robot = $this->createRobot();
        $this->loadMockPage($robot, self::formPageWithInputTypeText());
        $errorMsg = 'Submit failed';
        try {

            // This request is expected to throw an error with message
            // equal to the $errorMsg as response body will not match the
            // regex given in "test" option
            $robot->submit([
                'errorMessage' => $errorMsg,
                'test' => '/XXX/'
            ]);
        } catch (Throwable $e) {
            if (!preg_match("/$errorMsg/", $e->getMessage())) {
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' =>
                            "Expected error message '$errorMsg ...' found: $e"
                    ]);
            }
        }
    }

    /**
     * Tests submit() function with "outputFile" option. This function checks if
     * Robot stores the result to a file set by "outputFile" option.  This
     * function uses RobotSuite::checkSubmit() to run following sequence of
     * operations:
     *   - Loads the HTML page with form containing input type "text"
     *   - Submit request with post data and "outputFile" option
     *   - Read file from content and assert the response
     */
    public function testSubmitWithOutputFileOption()
    {
        $temp = File::temp();

        // Submit request with "outputFile" option
        $this->checkSubmit(
            $this->createRobot(),
            self::formPageWithInputTypeText(),
            [
                'fields' => ['name' => 'carl'],
                'outputFile' => $temp
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'name',
                            'value' => 'carl'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with postData option containing value for setting
     * input type "text". This function uses RobotSuite::checkSubmit() to run
     * following sequence of operations:
     *   - Loads the HTML page with form containing input type "text"
     *   - Submit request with post data to set form field of type "text" value
     *   - Assert the response
     */
    public function testSubmitWithTextField()
    {
        $this->checkSubmit(
            $this->createRobot(),
            self::formPageWithInputTypeText(),
            [
	           'fields' =>
                    [
                        'name' => 'carl'
                    ]
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'name',
                            'value' => 'carl'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with postData option containing value for setting
     * input type "password". This function uses RobotSuite::checkSubmit() to
     * run following sequence of operations:
     *   - Loads the HTML page with form containing input type "password"
     *   - Submit request with post data to set form field of type "password"
     *     value
     *   - Assert the response
     */
    public function testSubmitWithPasswordField()
    {
        $this->checkSubmit(
            $this->createRobot(),
            self::formPageWithInputTypePassword(),
            [
	           'fields' =>
                    [
                        'password' => 'xxx123'
                    ]
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'password',
                            'value' => 'xxx123'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with postData option containing value setting
     * input type "file". This function uses RobotSuite::checkSubmit() to run
     * following sequence of operations:
     *   - Loads the HTML page with form containing input type "file"
     *   - Submit request with post data to set form field of type "file" value
     *   - Assert the response
     */
    public function testSubmitWithFile()
    {
        $temp = File::temp();
        file_put_contents($temp, 'Test content');
        $this->checkSubmit(
            $this->createRobot(),
            self::formPageWithInputTypeFile(),
            [
	           'fields' =>
                    [
                        'file' =>
                            [
                                'path' => $temp,
                                'filename' => 'testfile.txt',
                                'contentType' => 'text/plain'
                            ]
                    ]
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'multipart/form-data',
                'form' =>
                    [
                        [
                            'name' => 'file',
                            'contentType' => 'text/plain' ,
                            'filename' => 'testfile.txt',
                            'value' => file_get_contents($temp)
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with postData option containing value for setting
     * input type "select". This function uses RobotSuite::checkSubmit() to run
     * following sequence of operations:
     *   - Loads the HTML page with form containing input type "select"
     *   - Submit request with post data to set form field of type "select"
     *     value
     *   - Assert the response
     */
    public function testSubmitWithSelectField()
    {
        $this->checkSubmit(
            $this->createRobot(),
            self::formPageWithInputTypeSelect(),
            [
	           'fields' =>
                    [
                        'color' => 'red'
                    ]
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'color',
                            'value' => 'red'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with postData option containing value for setting
     * input type "select" that allow multiple selection. This function uses
     * RobotSuite::checkSubmit() to run following sequence of operations:
     *   - Loads the HTML page with form containing input type "select" that
     *     allow multiple selections
     *   - Submit request with post data to set form field of type "select" value
     *   - Assert the response
     */
    public function testSubmitWithMultiSelectField()
    {
        $this->checkSubmit(
            $this->createRobot(),
            self::formPageWithInputTypeMultiSelect(),
            [
	           'fields' =>
                    [
                        'colors' => [ 'yellow', 'red' ]
                    ]
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'colors[0]',
                            'value' => 'yellow'
                        ],
                        [
                            'name' => 'colors[1]',
                            'value' => 'red'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with buttonName option where 2 form contain
     * buttons with same name. This function uses RobotSuite::checkSubmit() to
     * run following sequence of operations once with 1st form then with 2nd
     * form:
     *   - Loads the HTML page with multiple forms containing buttons
     *   - Submit request with post data containing form fields, form name
     *     and button name
     *   - Assert the response
     */
    public function testSubmitWithButtonNameOption()
    {
        $robot = $this->createRobot();

        // Select 2nd form and submit
        $this->checkSubmit(
            $robot,
            self::formPageWithMultipleButtons(),
            [
                'fields' =>
                    [
                        'name2' => 'carl'
                    ],
                'formName' => 'testform2', // Selects form to select button from
                'buttonName' => 'submit' // Selects submit button of "testform2"
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'submit',
                            'value' => 'submit2_value'
                        ],
                        [
                            'name' => 'name2',
                            'value' => 'carl'
                        ]
                    ]
            ]
        );

        // Select 1st form and submit
        $this->checkSubmit(
            $robot,
            self::formPageWithMultipleButtons(),
            [
                'fields' =>
                    [
                        'name1' => 'carl'
                    ],
                'formName' => 'testform1', // Selects form to select button from
                'buttonName' => 'submit'   // Selects submit button of "testform1"
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'submit',
                            'value' => 'submit1_value'
                        ],
                        [
                            'name' => 'name1',
                            'value' => 'carl'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with buttonId option where form contain
     * 2 buttons with same name but different ids. This function uses
     * RobotSuite::checkSubmit() to run following sequence of operations:
     *   - Loads the HTML page with multiple forms containing buttons
     *   - Submit request with post data containing form field and button id
     *   - Assert the response
     */
    public function testSubmitWithButtonIdOption()
    {
        $robot = $this->createRobot();
        $this->checkSubmit(
            $robot,
            self::formPageWithMultipleButtons(),
            [
                'fields' =>
                    [
                        'name1' => 'carl'
                    ],
                'buttonId' => 'cancel1_id' // Id of button from current form
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'cancel',
                            'value' => 'cancel1'
                        ],
                        [
                            'name' => 'name1',
                            'value' => 'carl'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with buttonIndex where form contains 3 buttons.
     * This function uses RobotSuite::checkSubmit() to run following sequence of
     * operations:
     *   - Loads the HTML page with multiple forms containing buttons
     *   - Submit request with post data containing form field and button index
     *     of the 3rd button in the form
     *   - Assert the response
     */
    public function testSubmitWithButtonIndexOption()
    {
        $this->checkSubmit(
            $this->createRobot(),
            self::formPageWithMultipleButtons(),
            [
                'fields' =>
                    [
                        'name1' => 'carl'
                    ],
                'buttonIndex' => 3 // Index of button of current form
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'cancel',
                            'value' => 'cancel2'
                        ],
                        [
                            'name' => 'name1',
                            'value' => 'carl'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with "formName" option. This function uses
     * RobotSuite::checkSubmit() to run following sequence of operations once
     * with 1st form then with 2nd form:
     *   - Loads the HTML page with multiple forms
     *   - Submit request with post data containing form fields and from name
     *   - Assert the response
     */
    public function testSubmitWithFormNameOption()
    {
        $robot = $this->createRobot();

        // Select first from using 'formName' option
        $this->checkSubmit(
            $robot,
            self::multiFormPage(),
            [
                'fields' =>
                    [
                        'name1' => 'carl'
                    ],
                'formName' => 'testform1'
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'name1',
                            'value' => 'carl'
                        ]
                    ]
            ]
        );

        // Go back to HTML containing forms
        $robot->back();

        // Select second from using 'formName' option
        $this->checkSubmit(
            $robot,
            self::multiFormPage(),
            [
                'fields' =>
                    [
                        'name2' => 'carl'
                    ],
                'formName' => 'testform2'
            ],
            [
                'method' => 'POST',
                'statusCode' => 200,
                'reasonPhrase' => 'OK',
                'contentType' => 'application/x-www-form-urlencoded',
                'form' =>
                    [
                        [
                            'name' => 'name2',
                            'value' => 'carl'
                        ]
                    ]
            ]
        );
    }

    /**
     * Tests submit() function with default fetch attempts and fetch multipler
     */
    public function testSubmitWithMultipleFetchAttempts()
    {
        if ($this->skipLongCases()) {
            return;
        }

        $robot = new BasicRobot();
        $url = self::mockUrl();
        $log = [];
        $rl =
            new \CodeRage\Tool\Test\MockRequestLogger(
                    [
                        function($robot, $method, $uri)
                        {
                            // No op
                        }
                    ],
                    [
                        function($robot, $method, $uri, $error)
                        {
                            if ($error === null &&
                                $robot->response()->getStatusCode() !== 200)
                            {
                                throw new
                                    Error([
                                        'status' => 'RETRY',
                                        'message' => 'Request failed'
                                    ]);
                            }
                        }
                    ]
                );
        $robot->registerRequestLogger($rl);
        $time = Time::real();
        $offset = 3; // Offset to add to the current time to set
                     // 'ifRequestedBefore' option of the first time based
                     // request and to 'ifRequestedAfter' option of second time
                     // time based request
        $page =
            [
                [
                    'statusCode' => '404',
                    'reasonPhrase' => 'Not Found',
                    'contentType' => 'text/html',
                    'ifRequestedAfter' => $time,
                    'ifRequestedBefore' => $time + $offset
                ],
                [
                    'body' => 'This this body text for third request',
                    'ifRequestedAfter' => $time + $offset
                ]
            ];
        $query = BracketObjectNotation::encodeAsQuery($page);
        if (isset($_SERVER['DBGP_IDEKEY']))
            $query .= '&XDEBUG_SESSION_START=' . $_SERVER['DBGP_IDEKEY'];

        // GET request is expected to repeat an operation multiple time until
        // the response code is 200. Follwing sequence of operations are run:
        //   - Call to preRequest() method is made.
        //   - Get request is made and which is expected to return status
        //     code 404
        //   - Call to postRequest() method is made which throws an exception
        //     with status 'RETRY' as status code of response is not '200'
        //   - Call to postRequest() method from error handle is made.
        // In error handle as error status was 'RETRY', operation will be
        // repeated after exponential backoff sleep time. If time is still
        // less then 'ifRequestedAfter' of second time based request then above
        // sequence of operations are repeated otherwise run:
        //   - Call to preRequest() method is made.
        //   - Get request is made and which is expected to return status
        //     code 200
        //   - Call to postRequest() method is made.
        $robot->get("$url?$query");
    }

            /*
             * Test back() function
             */

    // This Robot::back() has been tested in setForm() functions

            /*
             * Test Utility methods
             */

    /**
     * Tests wrongPage() function with string error message
     */
    public function testWrongPageWithStringErrorMessage()
    {
        $robot = $this->createRobot();
        $errorMsg = 'Wrong page loaded';
        try {
            $robot->get(self::mockUrl(['contentType' => 'text/html']));

            // This function is expected to throw an error with message equal to
            // $errorMsg
            $robot->wrongPage($errorMsg);
        } catch (Throwable $e) {
            if ($errorMsg !== $e->getMessage())
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' =>
                            "Expected error message '$errorMsg ...' found: $e"
                    ]);
        }
    }

    /**
     * Tests wrongPage() function with all possible options
     */
    public function testWrongPageWithErrorOptions()
    {
        $robot = $this->createRobot();
        $errorMsg = 'Wrong page loaded';
        $content = 'Test content';
        try {
            $robot->get(self::mockUrl([
                'contentType' => 'text/html'
            ]));

            // This function is expected to throw an error with message equal to
            // $errorMsg
            $robot->wrongPage([
                'status' => 'UNEXPECTED_CONTENT',
                'message' => $errorMsg,
                'details' => $errorMsg,
                'content' => $content,
                'contentType' => 'text/plain'
            ]);
        } catch (Throwable $e) {

            // Check error message
            if ($errorMsg !== $e->getMessage())
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' =>
                            "Expected error message '$errorMsg' found: $e"
                    ]);

            // Check error details
            if (!preg_match("/$errorMsg: see (.*)/", $e->details(), $match))
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' =>
                            "Expected error message '$errorMsg' found: $e"
                    ]);

            // Check content of file
            $identifier = $match[1];
            Assert::equal(ContentRecorder::getContent($identifier), $content);
        }
    }

        /**
         * Tests recordContent() function
         */

    // This function has been tested as part of testSetAndGetContentRecorder() test case

        /**
         * Helper methods
         */

    private function createRobot()
    {
        $robot = new BasicRobot($this->robotOptions);
        $robot->setContentRecorder(new ContentRecorder);
        return $robot;
    }

    /**
     * Make a GET request to ECHO_FORM_DATA with the given robot, query
     * parameters and options and assert the response
     *
     * @param CodeRage\Tool\Robot $robot A robot
     * @param array $options An array of request options to pass to
     *   Robot::get();
     * @param array $expected The expected response, as an array with keys
     *   "statusCode", "reasonPhrase", "contentType", and optionally "cookies"
     * @throws CodeRage\Error
     */
    private function checkGet($robot, $options, $expected = null)
    {
        $file = null;
        if (isset($options['outputFile'])) {
            $file = $options['outputFile'];
        }
        $robot->get(self::httpechoUrl(), $options);
        if ($expected !== null) {
            $this->checkResponse($robot, $expected, $file);
        }
    }

    /**
     * Make a POST request to ECHO_FORM_DATA with the given robot and options
     * and assert the response
     *
     * @param CodeRage\Tool\Robot $robot A robot
     * @param array $options An array of request options to pass to
     *   Robot::post();
     * @param array $expected The expected response, as an array with keys among
     *   "statusCode", "reasonPhrase", "contentType", and optionally "formData"
     * @throws CodeRage\Error
     */
    private function checkPost($robot, $options, $expected = null)
    {
        $file = null;
        if (isset($options['outputFile'])) {
            $file = $options['outputFile'];
        }
        $url = self::httpechoUrl();
        $robot->post($url, $options);
        if ($expected !== null) {
            $this->checkResponse($robot, $expected, $file);
        }
    }

    /**
     * Make a GET request to MOCK_RESPONSE to load given html page and then
     * submit the request with given robot and options and assert the response
     *
     * @param CodeRage\Tool\Robot $robot A robot
     * @param array $page An associative array of options passed to
     *   'mock-response.php' to generate HTML page
     * @param array $options An array of request options to pass to
     *   Robot::submit();
     * @param array $expected The expected response, as an array with keys
     *   "statusCode", "reasonPhrase", "contentType", and optionally "formData"
     * @throws CodeRage\Error
     */
    private function checkSubmit($robot, $page, $options, $expected = null)
    {
        $file = null;
        if (isset($options['outputFile'])) {
            $file = $options['outputFile'];
        }
        $this->loadMockPage($robot, $page);
        $robot->submit($options);
        if ($expected !== null) {
            $this->checkResponse($robot, $expected, $file);
        }
    }

    /**
     * Throws an exception if the response object associated with the given
     * robot doesn't match the expected value
     *
     * @param CodeRage\Tool\Robot $robot
     * @param array $expected The expected response, as an array with keys
     *   "statusCode", "reasonPhrase", "contentType", and optionally "headers"
     *    and "fields"
     * @param string $file A file path to read response from (optional)
     */
    private function checkResponse($robot, $expected, $file = null)
    {
        $actual = $this->decodeHttpEchoResponse($robot, $file);
        if (!isset($expected['headers'])) {
            unset($actual['headers']);
        } else {
            $this->validateHeaders(
                $actual['headers'],
                $expected['headers']
            );
            unset($actual['headers']);
            unset($expected['headers']);
        }
        if ( preg_match(self::MATCH_MULTIPART_FORM_DATA, $actual['contentType'] ?? '') &&
             preg_match(self::MATCH_MULTIPART_FORM_DATA, $expected['contentType'] ?? '') )
        {
            unset($actual['contentType']);
            unset($expected['contentType']);
        }
        unset($actual['uri']);
        unset($actual['cookies']);
        Assert::equal($actual, $expected);
    }

    /**
     * Checks if expected headers in $expected exists in list of headers in
     * $found
     *
     * @param array $found An associative array mapping header names to its
     *   values
     * @param array $expected An associative array mapping header names to its
     *   values
     * @throws Error
     */
    private function validateHeaders($found, $expected)
    {
		$lcFound = $lcExpected = [];
		foreach ($found as $n => $v)
			$lcFound[strtolower($n)] = $v;
		foreach ($expected as $n => $v)
			$lcExpected[strtolower($n)] = $v;
        foreach ($lcExpected as $n => $v) {
            if (!isset($lcFound[$n])) {
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' => "Missing header '$n'"
                    ]);
            } elseif (isset($lcFound[$n]) && $lcFound[$n] !== $v) {
                throw new
                    Error([
                        'status' => 'ASSERTION_FAILED',
                        'details' =>
                            "Invalid header value; Expected '$v' for " .
                            "header '$n'; found {$lcFound[$v]}"
                    ]);
            }
        }
    }

    /**
     * Checks the setForm() method by first setting second form using setForm()
     * and sending request and then setting first form and sending request.
     * This method uses the html page generated by self::multiFormPage() method
     *
     * @param array $options An associative array with keys among:
     *   form1 - An array mapping form selector type i.e id, name, selector,
     *     xpath to its value for the first form
     *   form2 - An array mapping form selector type i.e id, name, selector,
     *     xpath to its value for the second form
     * @throws Error
     */
    private function checkSetForm($options)
    {
        $robot = $this->createRobot();
        $page = self::multiFormPage();
        $this->loadMockPage($robot, $page);


        // Submit second form and check response
        $robot->setForm($options['form2']);
        $robot->submit();
        if (!preg_match('/form-2/', $robot->content()))
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'details' =>
                        "Expected a response containing 'from-2'; found " .
                        'response "' .  $robot->content() . '"'
                ]);

        // Go back
        $robot->back();

        // Submit first form and check response
        $robot->setForm($options['form1']);
        $robot->submit();
        if (!preg_match('/form-1/', $robot->content()))
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'details' =>
                        "Expected a response containing 'from-1'; found " .
                        'response "' .  $robot->content() . '"'
                ]);
    }

    /**
     * Tests setFields() function. This function uses run following sequence
     * of operations:
     *   - Make request to MOCK_RESPONSE that generate a HTML forms with name
     *     "testform", method "post", action https://httpecho.org, and inputs
     *      fields created with given "inputs" option
     *   - Sets values to input fields of form using given "values" option
     *   - Assert the response of the form using given "response" option
     *
     * @param array $options An associative array with keys among:
     *   inputs - An associative arrays of input types with same structure as
     *     required by MOCK_RESPONSE page
     *   values - An associative array of values passed to setField() method
     *   formResponse - An expected response as an associative array return by
     *     ECHO_FORM_DATA
     * @throws Error
     */
    private function checkSetFields($options)
    {
        $robot = $this->createRobot();

        // Load HTML page containing form
        $page =
            [
                'forms' =>
                    [
                        [
                            'name' => 'testform',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' => $options['inputs']
                        ]
                    ]
            ];
        $this->loadMockPage($robot, $page);

        // Set from fields
        $robot->setFields($options['values']);

        // Submit request
        $robot->submit();

        // Assert response
        $this->checkResponse($robot, $options['response']);
    }

    /**
     * Make a GET request to load given html page
     *
     * @param CodeRage\Tool\Robot $robot
     * @param array $page An associative array of options passed to
     *   'mock-response.php' to generate HTML page
     */
    private function loadMockPage($robot, $page)
    {
        $robot->get(self::mockUrl($page));
    }

    /**
     * Helper method for implementing "skip long cases" behavior
     *
     * @return boolean
     */
    private function skipLongCases()
    {
        if ($this->skipLongCases) {
            echo "Skipping long test case";
        }
        return $this->skipLongCases;
    }

    /**
     * Returns the url for the MOCK_RESPONSE page
     *
     * @return string
     */
    private static function mockUrl(array $params = [])
    {
        $config = \CodeRage\Config::current();
        $url =
            'https://' . $config->getRequiredProperty('site_domain') .
            self::MOCK_RESPONSE;
        if (isset($_SERVER['DBGP_IDEKEY']))
            $params['XDEBUG_SESSION_START'] = $_SERVER['DBGP_IDEKEY'];
        if (!empty($params))
            $url .= '?' . BracketObjectNotation::encodeAsQuery($params);
        return $url;
    }

    /**
     * Returns an appropriate httpbin URL
     *
     * @return string
     */
    private static function httpechoUrl($path = '')
    {
        return 'https://' . self::HTTP_ECHO_DOMAIN . '/' . $path;
    }

    /**
     * Returns the result of decoding the HTTP response from a request to
     * httpecho.org
     *
     * @param CodeRage\Tool\Robot $robot
     * @param string $file A file path to read response from (optional)
     */
    private function decodeHttpEchoResponse($robot, $file = null)
    {
        if (!$robot->hasContentType('application/json')) {
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'details' =>
                        "Incorrect content type: expected 'application/json'; " .
                        "found " . $robot->contentType()
                ]);
        }
        $content = $file !== null ? file_get_contents($file) : $robot->content();
        $data = Json::decode($content, ['objectsAsArrays' => true]);
        if ($data == Json::ERROR) {
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'message' => 'JSON decoding error: ' . Json::lastError()
                ]);
        }
        $response =
            [
                'uri' => $robot->request()->getUri(),
                'method' => $robot->request()->getMethod(),
                'statusCode' => $robot->response()->getStatusCode(),
                'reasonPhrase' => $robot->response()->getReasonPhrase()
            ];
        $contentType = $robot->request()->getHeader('Content-Type')[0] ?? null;
        if ($contentType !== null) {
            $response['contentType'] = $contentType;
        }
        foreach (self::HTTP_ECHO_PROPERTIES as $name => [$label, $type, $required]) {
            $value =
                Args::checkKey($data, $name, $type, [
                    'label' => $label,
                    'required' => $required
                ]);
            if ($value !== null) {
                $response[$name] = $value;
            }
        }
        if (isset($data['body'])) {
            $response['body'] = self::decodeDataUri($data['body']);
        }
        if (isset($data['form'])) {
            $form = [];
            foreach ($data['form'] as $n => $info) {
                Args::check($info, 'map[string]', 'form field metadata');
                $field = [];
                foreach (self::FORM_FIELD_PROPERTIES as $name => $v) {
                    [$label, $type, $required] = $v;
                    $value =
                        Args::checkKey($info, $name, $type, [
                            'label' => $label,
                            'required' => $required
                        ]);
                    if ($value !== null) {
                        $field[$name] = $name === 'value' ?
                            self::decodeDataUri($value) :
                            $value;
                    }
                }
                $form[] = $field;
            }
            $response['form'] = $form;
        }
        return $response;
    }

    /**
     * Returns a data URI storing the given data
     *
     * @param $data string The data
     * @return string
     */
    private function encodeDataUri($data)
    {
        if (mb_check_encoding($data, 'ASCII')) {
            $data = ',' . urlencode($data);
        } else {
            $data = ';base64,' . base64_encode($data);
        }
        return "data:$data";
    }

    /**
     * Returns the data stored by the given "data" URI, as a string
     *
     * @param string $uri A data URI without a media type component
     * @return string
     */
    private static function decodeDataUri($uri)
    {
        [$type, $base64, $content] =
            Regex::getMatch(self::MATCH_DATA_URI, $uri, [1, 2, 3]);
        if ($content === null) {
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'message' => "Invalid data URI: $uri"
                ]);
        }
        if ($type !== null) {
            throw new
                Error([
                    'status' => 'ASSERTION_FAILED',
                    'message' =>
                        "Invalid data URI '$uri': media types are not supported"
                ]);

        }
        return $base64 !== null ? base64_decode($content) : urldecode($content);
    }

    /**
     * Returns a regular expression matching the given text as it would appear
     * embedded in a data URI
     *
     * @param string $text The text
     * @return string
     */
    private static function dataUriRegex($text)
    {
        return '{' . quotemeta(urlencode($text)) . '}';
    }

    /**
     * Returns an associative array in the format of the JSON-encoded data
     * passed to MOCK_RESPONSE. The associative array will generate 2 HTML
     * forms with each from containing 1 input text field and 1 submit button
     *
     * @return array
     */
    private static function multiFormPage()
    {
        return
            [
                'forms' =>
                    [
                        [
                            'name' => 'testform1',
                            'id' => 'testform1_id',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' =>
                                [
                                    [
                                       'type' => 'text',
                                       'name' => 'name1',
                                       'value' => 'form-1'
                                   ],
                                   [
                                       'type' => 'submit',
                                       'name' => 'submit1'
                                   ],
                                ]
                        ],
                        [
                            'name' => 'testform2',
                            'id' => 'testform2_id',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' =>
                                [
                                    [
                                       'type' => 'text',
                                       'name' => 'name2',
                                       'value' => 'form-2'
                                   ],
                                   [
                                       'type' => 'submit',
                                       'name' => 'submit2'
                                   ],
                                ]
                        ]
                    ]
            ];
    }

    /**
     * Returns an associative array in the format of the JSON-encoded data
     * passed to MOCK_RESPONSE. The associative array will generate 1 HTML
     * form with input type file with name "file" along with intput type text and
     * submit button
     *
     * @return array
     */
    private static function formPageWithInputTypeFile()
    {
        return
            [
                'forms' =>
                    [
                        [
                            'name' => 'testform',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' =>
                                [
                                    [
                                        'type' => 'file',
                                        'name' => 'file'
                                    ],
                                    [
                                        'type' => 'submit',
                                        'name' => 'submit1'
                                    ],
                                ]
                        ],
                    ]
            ];
    }

    /**
     * Returns an associative array in the format of the JSON-encoded data
     * passed to MOCK_RESPONSE. The associative array will generate 1 HTML
     * form with input type select with name "color" along with submit button
     *
     * @return array
     */
    private static function formPageWithInputTypeSelect()
    {
        return
            [
                'forms' =>
                    [
                        [
                            'name' => 'testform',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' =>
                                [
                                    [
                                        'type' => 'select',
                                        'name' => 'color',
                                        'options' =>
                                            [
                                                [
                                                    'value' => 'yellow',
                                                    'label' => 'yellow'
                                                ],
                                                [
                                                    'value' => 'red',
                                                    'label' => 'red'
                                                ]
                                            ]
                                   ],
                                   [
                                       'type' => 'submit',
                                       'name' => 'submit'
                                   ],
                                ]
                        ],
                    ]
            ];
    }

    /**
     * Returns an associative array in the format of the JSON-encoded data
     * passed to MOCK_RESPONSE. The associative array will generate 1 HTML
     * form with input type multi-select with with name "color" along with
     * submit button
     *
     * @return array
     */
    private static function formPageWithInputTypeMultiSelect()
    {
        return
            [
                'forms' =>
                    [
                        [
                            'name' => 'testform',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' =>
                                [
                                    [
                                        'type' => 'select',
                                        'name' => 'colors',
                                        'multiple' => true,
                                        'options' =>
                                            [
                                                [
                                                    'value' => 'yellow',
                                                    'label' => 'yellow'
                                                ],
                                                [
                                                    'value' => 'red',
                                                    'label' => 'red'
                                                ]
                                            ]
                                   ],
                                   [
                                       'type' => 'submit',
                                       'name' => 'submit'
                                   ],
                                ]
                        ],
                    ]
            ];
    }

    /**
     * Returns an associative array in the format of the JSON-encoded data
     * passed to MOCK_RESPONSE. The associative array will generate 1 HTML
     * form with input type text with name "name" along with submit button
     *
     * @return array
     */
    private static function formPageWithInputTypeText()
    {
        return
            [
                'forms' =>
                    [
                        [
                            'name' => 'testform',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' =>
                                [
                                    [
                                        'type' => 'text',
                                        'name' => 'name',
                                   ],
                                   [
                                       'type' => 'submit',
                                       'name' => 'submit'
                                   ],
                                ]
                        ],
                    ]
            ];
    }

    /**
     * Returns an associative array in the format of the JSON-encoded data
     * passed to MOCK_RESPONSE. The associative array will generate 1 HTML
     * form with input type password with with name "password" along with
     * submit button
     *
     * @return array
     */
    private static function formPageWithInputTypePassword()
    {
        return
            [
                'forms' =>
                    [
                        [
                            'name' => 'testform',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' =>
                                [
                                   [
                                      'type' => 'password',
                                      'name' => 'password'
                                   ],
                                   [
                                       'type' => 'submit',
                                       'name' => 'submit'
                                   ],
                                ]
                        ],
                    ]
            ];
    }

    /**
     * Returns an associative array in the format of the JSON-encoded data
     * passed to MOCK_RESPONSE. The associative array will generate 2 HTML
     * forms where one form have 3 buttons and other form have 1 button. The
     * buttons have following properties:
     *   - First button of from 1 and 2 have same name and id i.e "submit" and
     *     "submit_id" but have different value "submit1_value"
     *   - Second and third button of from 1 and 2 have same name i.e "cancel"
     *     but differnet id and value
     *
     * @return array
     */
    private static function formPageWithMultipleButtons()
    {
        return
            [
                'forms' =>
                    [
                        [
                            'name' => 'testform1',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' =>
                                [
                                    [
                                        'type' => 'text',
                                        'name' => 'name1'
                                    ],
                                    [
                                         'type' => 'submit',
                                         'name' => 'submit',
                                         'id' => 'submit_id',
                                         'value' => 'submit1_value'
                                    ],
                                    [
                                         'type' => 'submit',
                                         'name' => 'cancel',
                                         'id' => 'cancel1_id',
                                         'value' => 'cancel1'
                                    ],
                                    [
                                         'type' => 'submit',
                                         'name' => 'cancel',
                                         'id' => 'cancel2_id',
                                         'value' => 'cancel2'
                                    ]
                                ]
                        ],
                        [
                            'name' => 'testform2',
                            'method' => 'post',
                            'action' => self::httpechoUrl(),
                            'inputs' =>
                                [
                                    [
                                        'type' => 'text',
                                        'name' => 'name2'
                                    ],
                                    [
                                         'type' => 'submit',
                                         'name' => 'submit',
                                         'id' => 'submit_id',
                                         'value' => 'submit2_value'
                                    ]
                                ]
                        ],
                    ]
            ];
    }

    /**
     * @var array
     */
    private $robotOptions;

    /**
     * @var boolean
     */
    private $skipLongCases;

}
