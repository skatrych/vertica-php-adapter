<?php
/**
 * DbTable class based on VerticaOdbcAdapter
 *
 * @author Sergii Katrych <sergii.katrych@westwing.de>
 * @date   19/11/15
 */

namespace VerticaPhpAdapter\Adapter\Wrapper;


use VerticaPhpAdapter\Adapter\Odbc\VerticaOdbcAbstract;
use VerticaPhpAdapter\Adapter\Query\QueryBuilder;
use VerticaPhpAdapter\Exceptions\OdbcException;

class DbTable
{
    protected $name;
    protected $schemaName;
    protected $adapter;

    /**
     * DbTable constructor.
     *
     * @param VerticaOdbcAbstract $adapter    Vertica adapter object
     * @param string              $tableName  Given table
     * @param string              $schemaName Schema name
     *
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function __construct(VerticaOdbcAbstract $adapter, $tableName, $schemaName = '')
    {
        $this->adapter = $adapter;
        $this->name = $tableName;
        $this->schemaName = $schemaName;
    }

    /**
     * Insert data into the table
     *
     * @param array $parameters Given parameters to insert
     *
     * @return bool
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function insert(array $parameters)
    {
        return $this->adapter->insert($this->name, $parameters);
    }

    /**
     * Update data in the table
     *
     * @param array  $parameters Given parameters to update
     * @param string $where      Given WHERE clause
     *
     * @return bool
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function update(array $parameters, $where)
    {
        return $this->adapter->update($this->name, $parameters, $where);
    }

    /**
     * Delete rows in the table.
     * Doesn't work with empty WHERE. You have to limit it by WHERE clause.
     *
     * @param string $where Given WHERE clause
     *
     * @return bool
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function delete($where)
    {
        return $this->adapter->delete($this->name, $where);
    }

    /**
     * Run a query
     *
     * @param string $sql Given SQL query
     *
     * @return ODBC resource
     * @throws OdbcException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function query($sql)
    {
        return $this->adapter->query($sql);
    }

    /**
     * Returns the list of table columns and types as an array
     *
     * @return array
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function describeTable()
    {
        return $this->adapter->describeTable($this->name, $this->schemaName);
    }

    /**
     * Returns QueryBuilder object to build select query
     *
     * @param array $fields Columns list to return
     *
     * @return QueryBuilder
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function select($fields = [])
    {
        return (new QueryBuilder($this->adapter, $this->name, $fields));
    }
}
