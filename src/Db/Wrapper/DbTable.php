<?php
/**
 * DbTable class based on VerticaOdbcAdapter
 *
 * @author Sergii Katrych <sergii.katrych@westwing.de>
 * @date   19/11/15
 */

namespace VerticaPhpAdapter\Db\Wrapper;


use VerticaPhpAdapter\Db\Odbc\VerticaOdbcAbstract;
use VerticaPhpAdapter\Db\Query\Builder as QueryBuilder;
use VerticaPhpAdapter\Exception\VerticaException;

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
        $this->useSchemaName();
    }

    /**
     * Insert data into the table
     *
     * @param array $parameters Given parameters to insert
     *
     * @return bool
     * @throws VerticaException
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
     * @throws VerticaException
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
     * Returns Builder object to build select query
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

    /**
     * Injecting schema name as a prefix of table name
     *
     * @return void
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function useSchemaName()
    {
        if (!empty($this->schemaName) || strpos($this->name, ".") > 0) {
            return;
        }
        $this->name = $this->schemaName . "." . $this->name;
    }
}
