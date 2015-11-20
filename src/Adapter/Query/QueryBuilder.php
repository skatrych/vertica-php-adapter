<?php
/**
 * Simple Query Builder for VerticaPhpAdapter
 *
 * @author Sergii Katrych <sergii.katrych@westwing.de>
 * @date   19/11/15
 */

namespace VerticaPhpAdapter\Adapter\Query;


use VerticaPhpAdapter\Adapter\Odbc\VerticaOdbcAbstract;

class QueryBuilder
{
    const VALUE_TYPE_STRING = 0;
    const VALUE_TYPE_NUMERIC = 1;
    const VALUE_TYPE_FUNC = 2;

    protected $adapter;
    protected $selectSQL = '';

    protected $whereString;
    protected $joinsString;
    protected $groupbyString;
    protected $orderbyString;
    protected $limitString;
    protected $selectFieldsList;
    protected $fromTable;

    public function __construct(VerticaOdbcAbstract $adapter, $tableName, $selectFieldsList = [])
    {
        $this->adapter = $adapter;
        $this->selectFieldsList = $selectFieldsList;
        $this->fromTable = $tableName;
    }

    public function where($condition)
    {
        $this->appendWhereString($condition);
        return $this;
    }

    public function andWhere($condition)
    {
        $this->appendWhereString(" AND ")->where($condition);
        return $this;
    }

    public function orWhere($condition)
    {
        $this->appendWhereString(" OR ")->where($condition);
        return $this;
    }

    public function leftJoin($table, $condition)
    {
        return $this->addJoin($table, $condition, 'LEFT');
    }

    public function rightJoin($table, $condition)
    {
        return $this->addJoin($table, $condition, 'RIGHT');
    }

    public function addJoin($table, $condition, $joinType)
    {
        $this->appendSql($this->filterJoinType($joinType))->
            appendSql(' JOIN ')->
            appendSql($table)->
            appendSql(' ON ')->
            appendSql("({$condition})");
        return $this;
    }

    public function groupBy($condition)
    {
        $this->groupbyString = $condition;
        return $this;
    }

    public function orderBy($column, $direction = "ASC")
    {
        $this->orderbyString .= !empty($this->orderbyString) ? ", " : "";
        $this->orderbyString .= $column . " " . (0 == strcasecmp($direction, "DESC") ? $direction : "ASC");
        return $this;
    }

    public function limit($offset, $limit)
    {
        $this->limitString = $this->adapter->limit($this->limitString, $limit, $offset);
    }

    public function fetchAll($fetchMode = VerticaOdbcAbstract::ARRAY_FETCH_MODE)
    {
        $sql = $this->buildSql();
        $resource = $this->adapter->query($sql);
        return $this->adapter->fetchAll($resource, $fetchMode);
    }

    protected function buildSql()
    {
        $this->selectSQL = "SELECT " . $this->buildFieldsList() . " FROM " . $this->fromTable;
        $this->selectSQL .= !empty($this->joinsString) ? " " . $this->joinsString : "";
        $this->selectSQL .= " WHERE " . (!empty($this->whereString) ? " " . $this->whereString : " 1");
        $this->selectSQL .= !empty($this->groupbyString) ? " GROUP BY " . $this->groupbyString : "";
        $this->selectSQL .= !empty($this->orderbyString) ? " ORDER BY " . $this->orderbyString : "";
        $this->selectSQL .= !empty($this->limitString) ? " " . $this->limitString : "";
        return $this->selectSQL;
    }

    protected function buildFieldsList()
    {
        if (empty($this->selectFieldsList)) {
            return "*";
        }

        return join(", ", $this->selectFieldsList);
    }

    protected function appendWhereString($string)
    {
        $this->whereString .= $string;
        return $this;
    }

    protected function appendSql($string)
    {
        $this->selectSQL .= $string;
        return $this;
    }

    protected function filterJoinType($type)
    {
        if (in_array($type, ['LEFT', 'RIGHT', 'INNER', 'OUTER'])) {
            return $type;
        }
        return '';
    }
}
