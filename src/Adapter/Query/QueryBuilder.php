<?php
/**
 * Super Simple Query Builder for VerticaPhpAdapter
 *
 * @author Sergii Katrych <sergii.katrych@westwing.de>
 * @date   19/11/15
 */

namespace VerticaPhpAdapter\Adapter\Query;


use VerticaPhpAdapter\Adapter\Odbc\VerticaOdbcAbstract;
use VerticaPhpAdapter\Exceptions\OdbcException;

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

    /**
     * QueryBuilder constructor.
     *
     * @param VerticaOdbcAbstract $adapter          Vertica Db adapter instance
     * @param string              $tableName        Given table name
     * @param array               $selectFieldsList List of columns/fields to select
     *
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function __construct(VerticaOdbcAbstract $adapter, $tableName, $selectFieldsList = [])
    {
        $this->adapter = $adapter;
        $this->selectFieldsList = $selectFieldsList;
        $this->fromTable = $tableName;
    }

    /**
     * Add condition to WHERE clause(s).
     * Please use it ONLY in case if this is the !first! WHERE condition.
     * Otherwise use andWhere/orWhere methods.
     *
     * @param string $condition WHERE condition (Example: "created_at > NOW()")
     *
     * @return $this
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function where($condition)
    {
        //@TODO: implement possibility to accept key/value pairs array and quote/escape values
        $this->appendWhereString($condition);
        return $this;
    }

    /**
     * Extension to QueryBuilder::where(),
     * adding " AND " before given condition.
     *
     * @param string $condition WHERE condition (Example: "created_at > NOW()")
     *
     * @return $this
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function andWhere($condition)
    {
        $this->appendWhereString(" AND ")->where($condition);
        return $this;
    }

    /**
     * Extension to QueryBuilder::where(),
     * adding " OR " before given condition.
     *
     * @param string $condition WHERE condition (Example: "created_at > NOW()")
     *
     * @return $this
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function orWhere($condition)
    {
        $this->appendWhereString(" OR ")->where($condition);
        return $this;
    }

    /**
     * Add LEFT JOIN with condition to the query
     *
     * @param string $table     Table to be joined
     * @param string $condition Condition on what we have to join the table
     *
     * @return QueryBuilder
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function leftJoin($table, $condition)
    {
        return $this->addJoin($table, $condition, 'LEFT');
    }

    /**
     * Add RIGHT JOIN with condition to the query
     *
     * @param string $table     Table to be joined
     * @param string $condition Condition on what we have to join the table
     *
     * @return QueryBuilder
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function rightJoin($table, $condition)
    {
        return $this->addJoin($table, $condition, 'RIGHT');
    }

    /**
     * Add LEFT/RIGHT/INNER/OUTER JOIN with condition to the query
     *
     * @param string $table     Table to be joined
     * @param string $condition Condition on what we have to join the table
     * @param string $joinType  LEFT/RIGHT/INNER/OUTER join
     *
     * @return QueryBuilder
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function addJoin($table, $condition, $joinType)
    {
        $this->appendSql($this->filterJoinType($joinType))->
            appendSql(' JOIN ')->
            appendSql($table)->
            appendSql(' ON ')->
            appendSql("({$condition})");
        return $this;
    }

    /**
     * Add GROUP BY condition to the query
     *
     * @param string $condition Group By condition
     *
     * @return $this
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function groupBy($condition)
    {
        $this->groupbyString = $condition;
        return $this;
    }

    /**
     * Add ORDER BY condition to the query
     *
     * @param string $column    Column name to order by
     * @param string $direction Direction ASC/DESC (ASC is used by default)
     *
     * @return $this
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function orderBy($column, $direction = "ASC")
    {
        $this->orderbyString .= !empty($this->orderbyString) ? ", " : "";
        $this->orderbyString .= $column . " " . (0 == strcasecmp($direction, "DESC") ? $direction : "ASC");
        return $this;
    }

    /**
     * Limit returned result set by given offset and max rows number
     *
     * @param int $offset Given offset
     * @param int $limit  Given Max rows number
     *
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function limit($offset, $limit)
    {
        $this->limitString = $this->adapter->limit($this->limitString, $limit, $offset);
    }

    /**
     * Fetch all rows from the result set according to given query
     *
     * @param int $fetchMode Fetching rows as Objects or Arrays (Default: array)
     *
     * @return array|object
     * @throws OdbcException
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    public function fetchAll($fetchMode = VerticaOdbcAbstract::ARRAY_FETCH_MODE)
    {
        $this->buildSql();

        try {
            $resource = $this->adapter->query($this->selectSQL);
        } catch (OdbcException $e) {
            throw new OdbcException("Failed to execute query: " . $this->selectSQL . "; due to " . $e->getMessage(), $e->getCode(), $e);
        }
        return $this->adapter->fetchAll($resource, $fetchMode);
    }

    /**
     * Building final SQL query based on given details from builder methods.
     *
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     * @return string
     */
    protected function buildSql()
    {
        $this->selectSQL = ''; // reset query string

        $this->appendSql("SELECT " . $this->buildFieldsList() . " FROM " . $this->fromTable)
            ->appendSql(!empty($this->joinsString) ? " " . $this->joinsString : "")
            ->appendSql(" WHERE " . (!empty($this->whereString) ? " " . $this->whereString : " 1"))
            ->appendSql(!empty($this->groupbyString) ? " GROUP BY " . $this->groupbyString : "")
            ->appendSql(!empty($this->orderbyString) ? " ORDER BY " . $this->orderbyString : "")
            ->appendSql(!empty($this->limitString) ? " " . $this->limitString : "");
    }

    /**
     * Building list of columns/fields to have in the result set
     *
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     * @return string
     */
    protected function buildFieldsList()
    {
        if (empty($this->selectFieldsList)) {
            return "*";
        }

        return join(", ", $this->selectFieldsList);
    }

    /**
     * Append WHERE condition to the current WHERE clause(s)
     * Note: don't add "WHERE" keywords as it will be added automatically.
     *
     * @param string $string WHERE clause to be added to the current one
     *
     * @return $this
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function appendWhereString($string)
    {
        $this->whereString .= $string;
        return $this;
    }

    /**
     * Append another piece of Query to the current one.
     *
     * @param string $string SQL query string to add
     *
     * @return $this
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function appendSql($string)
    {
        $this->selectSQL .= $string;
        return $this;
    }

    /**
     * Validate and filter out given JOIN type.
     * Returns empty string in case given type is not valid against whitelist.
     *
     * @param string $type Given JOIN type
     *
     * @return string
     * @author Sergii Katrych <sergii.katrych@westwing.de>
     */
    protected function filterJoinType($type)
    {
        if (in_array($type, ['LEFT', 'RIGHT', 'INNER', 'OUTER'])) {
            return $type;
        }
        return '';
    }
}
