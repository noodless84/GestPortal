<?php namespace Comodojo\Database\QueryBuilder;

use \Comodojo\Exception\DatabaseException;
use \Exception;

/**
 * STORE query builder
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

class QueryStore {

    private $model = null;

    private $table = null;

    private $values = null;

    private $keys = null;

    private $values_array = array();

    private $keys_array = array();

    public function __construct($model) {

        $this->model = $model;

    }

    final public function table($data) {

        $this->table = $data;

        return $this;

    }

    final public function values($data) {

        $this->values = $data;

        return $this;

    }

    final public function keys($data) {

        $this->keys = $data;

        return $this;

    }

    final public function valuesArray($data) {

        $this->values_array = $data;

        return $this;

    }

    final public function keysArray($data) {

        $this->keys_array = $data;

        return $this;

    }

    public function getQuery() {

        if ( is_null($this->table) OR empty($this->values) ) throw new DatabaseException('Invalid parameters for database->store', 1002);

        if ( sizeof($this->values_array) == 1 ) {

            $query_pattern = "INSERT INTO %s%s VALUES %s";

            $keys = ( $this->keys == "*" OR is_null($this->keys) ) ? null : "(".$this->keys.")";

            $query = sprintf($query_pattern, $this->table, " ".$keys, $this->values);

        }

        else {

            switch ($this->model) {

                case ("MYSQLI"):
                case ("MYSQL_PDO"):
                case ("POSTGRESQL"):
                case ("DB2"):
                case ("DBLIB_PDO"):

                    $query_pattern = "INSERT INTO %s%s VALUES%s";

                    $keys = ( $this->keys == "*" OR is_null($this->keys) ) ? null : "(".$this->keys.")";

                    $query = sprintf($query_pattern, $this->table, " ".$keys, " ".$this->values);

                    break;
                
                case ("SQLITE_PDO"):

                    $query_pattern = "INSERT INTO %s SELECT %s UNION SELECT %s";

                    if ( $this->keys == "*" OR is_null($this->keys) ) throw new DatabaseException('SQLite require expllicit keys definition in multiple insert statement');

                    $select = array();

                    foreach ($this->keys_array as $position => $key) array_push($select, $this->values_array[0][$position]." AS ".$key);

                    $union_select = array();

                    foreach (array_slice($this->values_array, 1) as $values) array_push($union_select, implode(", ",$values));

                    $query = sprintf($query_pattern, $this->table, implode(", ",$select), implode(" UNION SELECT ",$union_select));

                    break;
                    
                case ("ORACLE_PDO"):

                    $query_pattern = "INSERT INTO %s%s SELECT %s";

                    $keys = ( $this->keys == "*" OR is_null($this->keys) ) ? null : "(".$this->keys.")";

                    array_walk($this->values_array, function(&$value, $key) {

                        $value = "(".$value.")";

                    });

                    $values = implode(' FROM DUAL UNION ALL SELECT ', $this->values_array)." FROM DUAL";

                    $query = sprintf($query_pattern, $this->table, " ".$keys, $values);

                    break;

            }

        }

        return $query;

    }

}
