<?php
declare(strict_types = 1);

namespace Taxaos\GuzzleExamples;

require '../vendor/autoload.php';

use Exception;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Pool;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;

class AsynchronousConcurrentPoolRetry
{
    /**
     * @var int
     */
    protected $failed = 0;

    /**
     * @var int
     */
    protected $success = 0;

    /**
     * @var int
     */
    protected $timeout = 5;

    /**
     * @var int
     */
    protected $connectTimeout = 2;

    /**
     * @var int
     */
    protected $concurrency = 8;

    /**
     * @var int
     */
    protected $retries = 2;

    /**
     * @return int
     */
    public function getFailed()
    {
        return $this->failed;
    }

    /**
     * @return int
     */
    public function getSuccess()
    {
        return $this->success;
    }

    /**
     * @return int
     */
    public function getConcurrency()
    {
        return $this->concurrency;
    }

    /**
     * @param int $concurrency
     * @return $this
     */
    public function setConcurrency(int $concurrency)
    {
        $this->concurrency = $concurrency;

        return $this;
    }

    /**
     * @return int
     */
    public function getTimeout()
    {
        return $this->timeout;
    }

    /**
     * @param int $timeout
     * @return $this
     */
    public function setTimeout(int $timeout)
    {
        $this->timeout = $timeout;

        return $this;
    }

    /**
     * @return int
     */
    public function getRetries()
    {
        return $this->retries;
    }

    /**
     * @param int $retries
     * @return $this
     */
    public function setRetries(int $retries)
    {
        $this->retries = $retries;

        return $this;
    }

    /**
     * @return int
     */
    public function getConnectTimeout()
    {
        return $this->connectTimeout;
    }

    /**
     * @param int $connectTimeout
     * @return $this
     */
    public function setConnectTimeout(int $connectTimeout)
    {
        $this->connectTimeout = $connectTimeout;

        return $this;
    }

    /**
     * create closure which returns all prepared calls for chunk actions
     *
     * @return \Closure
     */
    protected function createClosureForRequestsList()
    {
        // create closure which returns all prepared calls for chunk actions
        // see yield keyword
        $requests = function ($client, $urls) {
            foreach ($urls as $url) {
                // create closure
                yield function () use ($client, $url) {
                    $this->log('Loading ' . $url);
                    /**
                     * @var $client Client
                     */
                    return $client->requestAsync(
                        'GET',
                        $url
                    );
                };
            }
        };

        return $requests;
    }

    /**
     * create http client with retry handler
     *
     * @return Client
     */
    protected function createHttpClientWithRetryHandler()
    {
        $stack = HandlerStack::create();
        $stack->push(Middleware::retry($this->createRetryHandler()));

        return new Client([
            'handler' => $stack,
            'connect_timeout' => $this->connectTimeout,
            'timeout' => $this->timeout,
        ]);
    }

    /**
     * process chunks async in a pool of requests (emits immediately new child, if one requests finishes)
     *
     * @param [] $urls
     */
    public function processChunksAsyncPool(array $urls)
    {
        // create closure
        /**
         * @var \Closure $requests
         */
        $requests = $this->createClosureForRequestsList();

        // create http client with RetryHandler
        $httpClient = $this->createHttpClientWithRetryHandler();

        $concurrency = $this->concurrency;

        // setup guzzle pool, pass client + created closure function with all needed params
        /**
         * @var Pool $pool
         */
        $pool = new Pool($httpClient, $requests($httpClient, $urls), [
            'concurrency' => $concurrency,
            'fulfilled' => function (Response $response, int $index) use ($urls) {
                $body = (string)$response->getBody();
                $size = strlen($body);
                $this->log(
                    'Got a successful response for url ' . $urls[$index] .
                    ' with index ' . $index .
                    ' with size ' . $size . ' bytes'
                );
                $this->success++;
            },
            'rejected' => function ($reason, $index) use ($urls) {
                /**
                 * @var Exception $reason
                 */
                $this->log(
                    'Got a NOT successful response for url ' . $urls[$index] .
                    ' with reason ' . $reason->getMessage()
                );
                $this->failed++;
                // do whatever you want with this information
            }
        ]);

        // Initiate the transfers and create a promise
        $promise = $pool->promise();

        // Force the pool of requests to complete.
        $promise->wait();
    }

    /**
     * create a retry handler for guzzle 6
     *
     * @return \Closure
     */
    protected function createRetryHandler()
    {
        return function (
            $retries,
            Request $request,
            Response $response = null,
            RequestException $exception = null
        ) {
            // set max retries from config
            $maxRetries = $this->retries;

            if ($retries >= $maxRetries) {
                return false;
            }

            if (!($this->isServerError($response) || $this->isConnectError($exception))) {
                return false;
            }

            $this->log(sprintf(
                'Retrying %s %s %s/%s, %s',
                $request->getMethod(),
                $request->getUri(),
                $retries + 1,
                $maxRetries,
                $response ? 'status code: ' . $response->getStatusCode() : $exception->getMessage()
            ));
            return true;
        };
    }

    /**
     * @param string $message
     */
    protected function log(string $message)
    {
        $time = new \DateTime('now');

        echo $time->format('c') . ' ' . $message . PHP_EOL;
    }

    /**
     * @param Response $response
     * @return bool
     */
    protected function isServerError(Response $response = null)
    {
        return $response && $response->getStatusCode() >= 400;
    }

    /**
     * @param RequestException $exception
     * @return bool
     */
    protected function isConnectError(RequestException $exception = null)
    {
        return $exception instanceof ConnectException;
    }
}

// list of urls
$urls = [
    'http://www.rapidshare.com/', //domain exists, but nothing connected on port 80
    'http://www.google.com/',
    'http://www.facebook.com/',
    'http://www.youtube.com/',
    'http://www.notexistingweirdstuff.com', //non-existing domain
    'http://www.yahoo.com/',
    'http://www.live.com/',
    'http://www.wikipedia.org/',
    'http://www.baidu.com/',
    'http://www.blogger.com/',
    'http://www.msn.com/',
    'http://www.qq.com/',
    'http://www.twitter.com/',
    'http://www.yahoo.co.jp/',
    'http://www.google.co.in/',
    'http://www.taobao.com/', // does not load sometimes
    'http://www.google.de/',
    'http://www.google.com.hk/',
    'http://www.wordpress.com/',
    'http://www.sina.com.cn/',
    'http://www.amazon.com/',
    'http://www.google.co.uk/',
    'http://www.microsoft.com/',
    'http://www.myspace.com/',
    'http://www.google.fr/',
    'http://www.bing.com/',
    'http://www.ebay.com/',
    'http://www.yandex.ru/',
    'http://www.google.co.jp/',
    'http://www.linkedin.com/',
    'http://www.google.com.br/',
    'http://www.163.com/',
    'http://www.mail.ru/',
    'http://www.flickr.com/',
    'http://www.craigslist.org/',
    'http://www.fc2.com/',
    'http://www.google.it/',
    'http://www.conduit.com/',
    'http://www.vkontakte.ru/',
    'http://www.google.es/'
];

try {
    $start = microtime(true);

    // create object, set concurrency, timeouts and retries
    $guzzle = new AsynchronousConcurrentPoolRetry();
    $guzzle->setConcurrency(20)
        ->setRetries(2)
        ->setConnectTimeout(2)
        ->setTimeout(10)
        ->processChunksAsyncPool($urls);

    $end = microtime(true);
    $time = $end - $start;

    // output some statistics
    echo count($urls) . ' urls done in ' . $time . 's' . PHP_EOL;
    echo 'success: ' . $guzzle->getSuccess() . PHP_EOL;
    echo 'failed: ' . $guzzle->getFailed() . PHP_EOL;
    echo 'concurrency: ' . $guzzle->getConcurrency() . PHP_EOL;
    echo 'speed per page: ' . $time / count($urls) . 's' . PHP_EOL;
    exit(0); //success
} catch (\Throwable $t) {
    echo $t->getMessage() . PHP_EOL;
    exit(1); //error
}