<?php namespace Comodojo\Database\QueryBuilder;

use \Comodojo\Exception\DatabaseException;
use \Exception;

/**
 * TRUNCATE query builder
 * 
 * @package     Comodojo Spare Parts
 * @author      Marco Giovinazzi <info@comodojo.org>
 * @license     GPL-3.0+
 *
 * LICENSE:
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

class QueryTruncate {

    private $model = null;

    private $table = null;

    public function __construct($model) {

        $this->model = $model;

    }

    final public function table($data) {

        $this->table = $data;

        return $this;

    }

    public function getQuery() {
        
        if ( is_null($this->table) ) throw new DatabaseException('Invalid parameters for database->empty',1016);

        switch ($this->model) {

            case ("MYSQLI"):
            case ("MYSQL_PDO"):
            case ("ORACLE_PDO"):
            case ("DBLIB_PDO"):

                $query_pattern = "TRUNCATE TABLE %s";

                break;

            case ("POSTGRESQL"):

                $query_pattern = "TRUNCATE %s RESTART IDENTITY";

                break;

            case ("DB2"):

                $query_pattern = "TRUNCATE TABLE %s IGNORE DELETE TRIGGERS DROP STORAGE IMMEDIATE";

                break;
            
            case ("SQLITE_PDO"):

                $query_pattern = "DELETE FROM %s; DELETE FROM SQLITE_SEQUENCE WHERE name='%s'";

                break;

        }

        if ( $this->model == "SQLITE_PDO" ) return sprintf($query_pattern, $this->table, $this->table);

        else return sprintf($query_pattern, $this->table);

    }

}
