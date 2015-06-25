<?php

require("database-config.php");
require("../src/Exception/DatabaseException.php");
require("../src/Database.php");
require("../src/EnhancedDatabase.php");

use Comodojo\Exception\DatabaseException;
use Comodojo\Database\Database;
use Comodojo\Database\EnhanceDatabase;

try {

    $db = new Database(
    	COMODOJO_DB_MODEL,
    	COMODOJO_DB_HOST,
		COMODOJO_DB_PORT,
		COMODOJO_DB_NAME,
		COMODOJO_DB_USER,
		COMODOJO_DB_PASSWORD
    );

}
catch (DatabaseException $de) {

    die("comodojo exception: ".$de->getMessage());

}
catch (Exception $e){

    die($e->getMessage());

}

echo 'Connected to database '.COMODOJO_DB_NAME.'::'.COMODOJO_DB_PORT;
