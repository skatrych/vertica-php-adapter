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

class DbTable
{
    protected $name;
    protected $schemaName;
    protected $adapter;

    public function __construct(VerticaOdbcAbstract $adapter, $tableName, array $config)
    {
        $this->adapter = $adapter;
        $this->name = $tableName;
        if (!empty($config['schemaname'])) {
            $this->schemaName = $config['schemaname'];
        }
    }

    public function insert(array $parameters)
    {
        return $this->adapter->insert($this->name, $parameters);
    }

    public function update(array $parameters, $where)
    {
        return $this->adapter->update($this->name, $parameters, $where);
    }

    public function delete($where)
    {
        return $this->adapter->delete($this->name, $where);
    }

    public function query($sql)
    {
        return $this->adapter->query($sql);
    }

    public function describeTable()
    {
        return $this->adapter->describeTable($this->name, $this->schemaName);
    }

    public function select($fields = [])
    {
        return (new QueryBuilder($this->adapter, $this->name, $fields));
    }
}
