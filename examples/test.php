<?php
/**
 * Simple example that show how you can use vertica-php-adapter (no DI)
 *
 * @author Sergii Katrych <sergii.katrych@westwing.de>
 * @date 23/11/15
 */

use VerticaPhpAdapter\Adapter\Wrapper\DbTable;
use VerticaPhpAdapter\Adapter\Wrapper\Zend_Db_Adapter;

$config = [
    'user' => 'dbuser',
    'password' => 'test',
    'database' => 'testDb',
    'host' => 'localhost'
];

$adapter = new Zend_Db_Adapter($config); // You can create any adapter class and extend it from VerticaOdbcAbstract to have functionality below
$dbTable = new DbTable($adapter, "testTable", "testSchema");

/*
 * Fetching data using DbTable and QueryBuilder
 */
try {
    $results = $dbTable->select(['column1', 'column2'])
        ->where('create_at > NOW()')
        ->orderBy('created_at', 'DESC')
        ->limit(5, 10)
        ->fetchAll();
    var_dump("Select with QueryBuilder returned: ", $results);

} catch (Exception $e) {
    var_dump("Data fetching failed due to {$e->getMessage()}");
}

/*
 * Inserting data using DbTable
 */
try {
    $result = $dbTable->insert(['column1' => 'value1', 'column2' => 'value2', 'created_at' => date('Y-m-d')]);
    var_dump("Insert operation completed with result: " . (true === $result ? 'OK' : 'NOK'));

} catch (Exception $e) {
    var_dump("Insert failed due to {$e->getMessage()}");
}

/*
 * Updating data using DbTable
 */
try {
    $result = $dbTable->update(['column1' => 'value1', 'column2' => 'value2', 'created_at' => date('Y-m-d')], "id_test = 123");
    var_dump("UPDATE operation completed with result: " . (true === $result ? 'OK' : 'NOK'));

} catch (Exception $e) {
    var_dump("Update failed due to {$e->getMessage()}");
}

/*
 * Or alternatively you can do all of the operations described above without using DbTable but adapter instance directly.
 * Please note: you have to specify table name in this case.
 * If you choose to use adapter directly, you don't have QueryBuilder functionality yet while fetching data.
 */

$connection = $adapter->connect();
if (false === $connection) {
    die("Can't connect to Vertica Db using vertica-php-adapter");
}

try {
    $result = $adapter->insert("testSchema.testTable", ['column1' => 'value1', 'column2' => 'value2', 'created_at' => date('Y-m-d')]);
    var_dump("Insert operation completed with result: " . (true === $result ? 'OK' : 'NOK'));

} catch (Exception $e) {
    var_dump("Insert failed due to {$e->getMessage()}");
}

// @TODO: add adapter-direct examples for the other methods (update/delete/query/fetchAll/fetchOne)
