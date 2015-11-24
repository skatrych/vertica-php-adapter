<?php

/**
 * Abstract Odbc adapter for PHP to communicate to Vertica
 *
 * @TODO: Implement schema name as a separate attribute for most of the methods to make it more flexible
 *
 * @author Sergii Katrych <sergii.katrych@westwing.de>
 * @date   17/11/15
 */

namespace VerticaPhpAdapter\Db\Odbc;

use Exception;
use VerticaPhpAdapter\Exception\VerticaConnectionException;
use VerticaPhpAdapter\Exception\VerticaException;
use VerticaPhpAdapter\Exception\VerticaQueryException;

abstract class VerticaOdbcAbstract
{
    const FETCH_MODE_ARRAY = 1;
    const FETCH_MODE_OBJECT = 2;

    /** @var array */
    protected $config;

    /** @var array */
    protected $requiredConfigProperties = [
        'host',
        'database',
        'user',
        'password'
    ];

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
     * @param array $config key/value pairs for Db config
     *
     * @throws Exception
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        if (false === $this->validateConfig()) {
            throw new VerticaException("Vertica Odbc Adapter Exception. Failed to validate config properties.");
        }

        $this->buildDsn();
    }

    /**
     * Returns db connection resource
     *
     * @return Resource|false
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function getConnection()
    {
        if (is_null($this->connection)) {
            try {
                $this->connect();
            } catch (VerticaConnectionException $e) {
                return false;
            }
        }
        return $this->connection;
    }

    /**
     * Fetch details about Db table
     *
     * @param string      $tableName  Table identifier
     * @param string|null $schemaName Schema identifier
     *
     * @return array
     * @throws VerticaQueryException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function describeTable($tableName, $schemaName = null)
    {
        if (null === $schemaName && strpos($tableName, ".") > 0) {
            list($schemaName, $tableName) = explode(".", $tableName);
        }

        $result = [];
        $query = $this->query("SELECT column_name, data_type FROM v_catalog.columns WHERE table_schema='{$schemaName}' AND table_name='{$tableName}' ORDER  BY ordinal_position;");
        $columns = $this->fetchAll($query);

        if (empty($columns)) {
            return $result;
        }

        foreach ($columns as $columnDetails) {
            $result[$columnDetails['column_name']] = $columnDetails['data_type'];
        }

        return $result;
    }

    /**
     * Executes "fetch data" related SQL query
     *
     * @param string $sql Query string
     *
     * @return resource
     * @throws VerticaQueryException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function query($sql)
    {
        $resource = odbc_exec($this->getConnection(), $sql);

        if (false === $resource) {
            throw new VerticaQueryException(odbc_errormsg($this->getConnection()), odbc_error($this->getConnection()));
        }

        return $resource;
    }

    /**
     * Fetch data based on query resource
     *
     * @param Resource $resource  Result of execution $this->query()
     * @param int      $fetchMode Optionally choose fetching mode: Object or Array
     * @param int|null $rowNumber Optionally choose which row number to retrieve
     *
     * @return array|object
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function fetchAll($resource, $fetchMode = self::FETCH_MODE_ARRAY, $rowNumber = null)
    {
        $results = [];

        $fetchFunc = (self::FETCH_MODE_OBJECT === $fetchMode) ? "odbc_fetch_object" : "odbc_fetch_array";
        while ($row = $fetchFunc($fetchFunc, $resource, $rowNumber)) {
            $results[] = $row;
        }

        return $results;
    }

    /**
     * Fetch single row based on query resource
     *
     * @param Resource $resource  Result of execution $this->query()
     * @param int      $fetchMode Optionally choose fetching mode: Object or Array
     *
     * @return array|object
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function fetchOne($resource, $fetchMode = self::FETCH_MODE_ARRAY)
    {
        return $this->fetchAll($resource, $fetchMode, 0);
    }

    /**
     * Insert new record into the table
     *
     * @param string $tableName  Given db table name with schema as a prefix (Example: 'schema.table')
     * @param array  $parameters List of column/value pairs
     *
     * @return bool
     * @throws VerticaQueryException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function insert($tableName, array $parameters)
    {
        $parameters = $this->filterBindingParams($tableName, $parameters);

        $sql = "INSERT INTO {$tableName} (" . join(", ", array_keys($parameters)) . ") VALUES (" . rtrim(str_repeat("?, ", count($parameters)), ",") . ")";

        array_unshift($parameters, $tableName);
        return $this->prepareAndExecute($sql, $parameters);
        // ODBC doesn't support lastInsertID() or similar.
    }

    /**
     * Update rows in the table with values from @parameters within WHERE clause
     *
     * @param string $tableName  Given db table name with schema as a prefix (Example: 'schema.table')
     * @param array  $parameters List of column/value pairs to replace existing values
     * @param string $where      WHERE clause
     *
     * @return bool
     * @throws VerticaQueryException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function update($tableName, array $parameters, $where)
    {
        $parameters = $this->filterBindingParams($tableName, $parameters);

        $sql = "UPDATE {$tableName} SET ";

        foreach ($parameters as $column => $value) {
            $sql .= $column . " = {$this->quote($value)},";
        }
        $sql = rtrim($sql, ',') . ' WHERE ' . (!empty($where) ? $where : '1');

        return $this->prepareAndExecute($sql);
    }

    /**
     * Delete from the table with specified WHERE clause(s)
     * !!! Doesn't allow to delete all records from the table by providing an empty $where !!!
     * For those cases please use $this->query() instead.
     *
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     * @param string $tableName Given db table name with schema as a prefix (Example: 'schema.table')
     * @param mixed  $where     WHERE clause, can be either string or array with columnName/value pairs
     *
     * @return bool
     * @throws VerticaQueryException
     */
    public function delete($tableName, $where)
    {
        $sql = "DELETE FROM {$tableName} WHERE ";

        switch (true) {
            case empty($where):
                return false;
                break;

            case is_string($where):
                $sql .= $where;
                $where = [];
                break;

            case is_array($where):
                $where = $this->filterBindingParams($tableName, $where);
                if (empty($where)) {
                    return false;
                }

                foreach ($where as $column => $value) {
                    $sql .= $column . ' = ?,';
                }
                $sql .= rtrim($sql, ',');
                break;
        }

        return $this->prepareAndExecute($sql, $where);
    }

    /**
     * Disable ODBC autocommit that is equivalent to starting transaction.
     *
     * @throws VerticaException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function beginTransaction()
    {
        $result = odbc_autocommit($this->getConnection(), false);

        if (false === $result) {
            throw new VerticaException("Failed to start transaction. Can't disable ODBC autocommit.", odbc_error($this->getConnection()));
        }
    }

    /**
     * Commit transaction and re-enable autocommit mode
     *
     * @throws VerticaException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function commit()
    {
        $result = odbc_commit($this->getConnection());
        if (false === $result) {
            throw new VerticaException("Failed to commit transaction due to " . odbc_errormsg($this->getConnection()), odbc_error($this->getConnection()));
        }

        $result = odbc_autocommit($this->getConnection(), true);
        if (false === $result) {
            throw new VerticaException("Failed to re-enable autocommit to get out of transactions mode. " . odbc_errormsg($this->getConnection()), odbc_error($this->getConnection()));
        }
    }

    /**
     * Roll back transaction
     *
     * @throws VerticaException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function rollback()
    {
        $result = odbc_rollback($this->getConnection());

        if (false === $result) {
            throw new VerticaException("Failed to RollBack transaction due to " . odbc_errormsg($this->getConnection()), odbc_error($this->getConnection()));
        }
    }

    /**
     * Adds Vertica specific LIMIT clause to the SELECT statement.
     *
     * @param mixed   $sql    Sql query
     * @param integer $count  Limit returned results to this number
     * @param integer $offset Query offset
     *
     * @return string Updated SQL query
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function limit($sql, $count, $offset = 0)
    {
        $sql .= $offset ? ' OFFSET ' . $offset : '';
        $sql .= $count ? ' LIMIT ' . $count : '';
        return $sql;
    }

    /**
     * Ping Db connection to see if it's alive
     *
     * @return bool
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function ping()
    {
        try {
            $res = $this->query("SELECT 1");
        } catch (VerticaQueryException $e) {
            return false;
        }

        $result = $this->fetchOne($res);
        return current($result) == 1;
    }

    /**
     * Connect to the database
     *
     * @return bool
     * @throws VerticaConnectionException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function connect()
    {
        if (!extension_loaded('odbc')) {
            throw new Exception("The ODBC extension is required for this adapter BUT it's not loaded.");
        }

        $this->connection = odbc_connect($this->config['dsn'], $this->config['user'], $this->config['password']);
        if (false === $this->connection) {
            throw new VerticaConnectionException("Can't connect to Vertica Database with DSN string " . $this->config['dsn']);
        }

        return true;
    }

    /**
     * Quote a raw value.
     *
     * @param string $value Raw value
     *
     * @return string
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function quote($value)
    {
        if (is_int($value) || is_float($value)) {
            return $value;
        }
        return "'" . addcslashes($value, "\000\n\r\\'\"\032") . "'";
    }

    /**
     * Make sure that all required config properties are set
     *
     * @return bool
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function validateConfig()
    {
        $requiredProperties = array_flip($this->requiredConfigProperties);

        $missingProperties = array_diff_key($requiredProperties, $this->config);
        return empty($missingProperties);
    }

    /**
     * Building proper DSN string
     *
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function buildDsn()
    {
        $driver = !empty($this->config['driver']) ? $this->config['driver'] : 'Vertica';
        $port = !empty($this->config['port']) ? $this->config['port'] : 5433;

        $this->config['dsn'] = "Driver={$driver};Servername={$this->config['host']};Port={$port};Database={$this->config['database']};";
    }

    /**
     * Filter out parameters that doesn't match to column names in the table
     *
     * @param string $tableName  Given db table
     * @param array  $parameters List of parameters to filter
     *
     * @return array
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function filterBindingParams($tableName, $parameters)
    {
        $tableColumns = $this->describeTable($tableName);

        return array_filter($parameters, function ($key) use ($tableColumns) {
            return array_key_exists($key, $tableColumns);
        }, ARRAY_FILTER_USE_KEY);
    }

    /**
     * Prepares SQL query as a statement and executes it with bind parameters
     *
     * @param string $sql        Given SQL query
     * @param array  $parameters Parameters to bind (optional in case you don't have placeholders in your query)
     *
     * @return bool
     * @throws VerticaQueryException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function prepareAndExecute($sql, array $parameters = array())
    {
        $stmt = odbc_prepare($this->getConnection(), $sql);

        if (false === $stmt) {
            throw new VerticaQueryException(odbc_errormsg($this->getConnection()), odbc_error($this->getConnection()));
        }

        // @TODO: validate and quote $parameters values
        $result = odbc_execute($stmt, $parameters);

        if (false === $result) {
            return false;
        }

        return odbc_num_rows($stmt);
    }
}
