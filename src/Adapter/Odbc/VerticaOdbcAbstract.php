<?php

/**
 * Abstract Odbc adapter for PHP to communicate to Vertica
 *
 * @author Sergii Katrych <sergii.katrych@westwing.de>
 * @date   17/11/15
 */

namespace VerticaPhpAdapter\Adapter\Odbc;

use Exception;

abstract class VerticaOdbcAbstract
{
    /** @var array */
    protected $config;

    /** @var Resource */
    protected $connection = null;

    /**
     * VerticaOdbcAbstract constructor.
     *
     * Example: array(
     *  'user' => 'string',
     *  'password' => 'string',
     *  'database' => 'string',
     *  'host' => 'string',
     *  'driver' => 'Vertica', // optional
     *  'port' => '5433' // optional
     * )
     *
     * @param array $config Db config
     *
     * @throws Exception
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        if (false === $this->validateConfig()) {
            throw new Exception("Vertica Odbc Adapter Exception. Failed to validate config properties.");
        }

        $this->buildDsn();
    }

    public function getConnection()
    {
        if (is_null($this->connection)) {
            $this->connect();
        }
        return $this->connection;
    }

    public function connnect()
    {
        $this->connection = odbc_connect($this->config['dsn'], $this->config['user'], $this->config['password']);
        if (false === $this->connection) {
            return false;
        }

        return true;
    }

    protected function validateConfig()
    {
        // @TODO: validate config for required properties
        return true;
    }

    protected function buildDsn()
    {
        $this->config['dsn'] = "Driver=Vertica;Servername=192.168.1.1;Port=5433;Database=dbname;";
    }
}