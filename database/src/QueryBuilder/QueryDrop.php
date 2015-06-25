<?php namespace Comodojo\Database\QueryBuilder;

use \Comodojo\Exception\DatabaseException;
use \Exception;

/**
 * DROP query builder
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

class QueryDrop {
  
    private $model = null;

    private $if_exists = null;

    private $table = null;

    public function __construct($model) {

        $this->model = $model;

    }

    final public function ifExists($data) {
    
        $this->if_exists = $data;
    
        return $this;
    
    }
    
    final public function table($data) {
    
        $this->table = $data;
    
        return $this;
    
    }

    public function getQuery() {
        
        if ( is_null($this->table) ) throw new DatabaseException('Invalid parameters for database->drop',1023);
        
        $query_pattern = "DROP TABLE %s%s";

        if ( in_array($this->model, array("MYSQL","MYSQLI","MYSQL_PDO","POSTGRESQL","DBLIB_PDO","ORACLE_PDO","SQLITE_PDO")) ) $if_exists = is_null($this->if_exists) ? null : 'IF EXISTS ';

        else $if_exists = null;

        return sprintf($query_pattern, $if_exists, $this->table);

    }

}
