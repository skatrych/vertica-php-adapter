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

class DbTable extends VerticaOdbcAbstract
{
    protected $name;
    protected $schemaName;

    public function __construct($tableName, array $config)
    {
        $this->name = $tableName;
        parent::__construct($config);
    }

    public function insert(array $parameters)
    {
        return parent::insert($this->name, $parameters);
    }

    public function update(array $parameters, $where)
    {
        return parent::update($this->name, $parameters, $where);
    }

    public function delete($where)
    {
        return parent::delete($this->name, $where);
    }

    public function describeTable()
    {
        return parent::describeTable($this->name, $this->schemaName);
    }

    public function select($fields = [])
    {
        return (new QueryBuilder($this, $this->name, $fields));
    }
}