<?php

require("database-config.php");
require("../src/Exception/DatabaseException.php");
require("../src/Database.php");
require("../src/EnhancedDatabase.php");
require("../src/QueryBuilder/Column.php");
require("../src/QueryBuilder/QueryCreate.php");
require("../src/QueryBuilder/QueryDelete.php");
require("../src/QueryBuilder/QueryDrop.php");
require("../src/QueryBuilder/QueryEmpty.php");
require("../src/QueryBuilder/QueryGet.php");
require("../src/QueryBuilder/QueryStore.php");
require("../src/QueryBuilder/QueryUpdate.php");

use Comodojo\Exception\DatabaseException;
use Comodojo\Database\Database;
use Comodojo\Database\EnhancedDatabase;
use Comodojo\Database\QueryBuilder\Column;

$query_output_pattern = '
    <h1> __NAME__ </h1>
    <p> Query should like: </p>
    <pre><code> __TEXT__ </code></pre>
    <p> QueryBuilder returns: </p>
    <pre><code> __BUILDER__ </code></pre>
';

$query_case = array();

try {

    $db = new EnhancedDatabase(
    	COMODOJO_DB_MODEL,
    	COMODOJO_DB_HOST,
		COMODOJO_DB_PORT,
		COMODOJO_DB_NAME,
		COMODOJO_DB_USER,
		COMODOJO_DB_PASSWORD
    );

    $db->autoClean();

    array_push($query_case, array(
        'name'      =>  "Simple SELECT (GET)",
        'text'      =>  "SELECT this,is FROM test",
        'builder'   =>  $db->table('test')->keys(array('this','is'))->getQuery("GET")
    ));

    array_push($query_case, array(
        'name'      =>  "Simple SELECT DISTINCT (GET)",
        'text'      =>  "SELECT DISTINCT this,is FROM test",
        'builder'   =>  $db->table('test')->distinct()->keys(array('this','is'))->getQuery("GET")
    ));

    array_push($query_case, array(
        'name'      =>  "Simple SELECT with simple where condition (GET)",
        'text'      =>  "...",
        'builder'   =>  $db->table('test')->keys(array('this','is'))->where('this','=','test')->getQuery("GET")
    ));

    array_push($query_case, array(
        'name'      =>  "Simple SELECT with double where condition (GET)",
        'text'      =>  "...",
        'builder'   =>  $db->table('test')->keys(array('this','is'))->where('this','=','test')->andWhere('is','!=','foo')->getQuery("GET")
    ));

    array_push($query_case, array(
        'name'      =>  "Simple SELECT with complex where condition (GET)",
        'text'      =>  "...",
        'builder'   =>  $db->table('test')->keys(array('this','is'))->where(array('this','LIKE','test%'),'OR',array('this','IN',array(1,10,'boo','koo')))->orWhere('is','!=','foo')->getQuery("GET")
    ));

    array_push($query_case, array(
        'name'      =>  "Simple SELECT with very complex where condition (GET)",
        'text'      =>  "...",
        'builder'   =>  $db->table('test')->keys(array('this','is','bla','cra'))
            ->where(array('this','LIKE','test%'),
                'OR',
                array('this','IN',array(1,10,'boo','koo')))
            ->orWhere('is','!=','foo')
            ->andWhere(
                array('bla','NOT BETWEEN',array(1,10000)),
                'AND',
                array('cra','IS NOT',null)
            )->getQuery("GET")
    ));

    array_push($query_case, array(
        'name'      =>  "SELECT with inner join condition (GET)",
        'text'      =>  "...",
        'builder'   =>  $db->table('test')->keys(array('this','is'))->join('INNER','test2','t2')->getQuery("GET")
    ));

    $column_1 = new Column('foo','STRING');
    $column_2 = new Column('koo','INTEGER');
    $column_3 = new Column('id','INTEGER');

    array_push($query_case, array(
        'name'      =>  "CREATE table",
        'text'      =>  "...",
        'builder'   =>  $db
            ->column($column_3->length(32)->unsigned()->notNull()->autoIncrement()->primaryKey())
            ->column($column_1->length(128)->defaultValue(NULL))
            ->column($column_2->length(64)->notNull()->unsigned())
            ->getQuery("CREATE", array(
                "name" => 'testTable',
                "if_not_exists" => true,
                "engine" => null
            ))
    ));

    array_push($query_case, array(
        'name'      =>  "DROP table",
        'text'      =>  "...",
        'builder'   =>  $db->table('testTable')->getQuery("DROP")
    ));

    array_push($query_case, array(
        'name'      =>  "EMPTY table",
        'text'      =>  "...",
        'builder'   =>  $db->table('testTable')->getQuery("EMPTY")
    ));

}
catch (DatabaseException $de) {

    die("comodojo exception: ".$de->getMessage());

}
catch (Exception $e){

    die($e->getMessage());

}

foreach ($query_case as $case) {
    
    echo str_replace(array('__NAME__','__TEXT__','__BUILDER__'), array($case['name'],$case['text'],$case['builder']), $query_output_pattern);

}
