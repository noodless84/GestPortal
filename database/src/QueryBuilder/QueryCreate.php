<?php namespace Comodojo\Database\QueryBuilder;

use \Comodojo\Exception\DatabaseException;
use \Exception;

/**
 * CREATE query builder
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

class QueryCreate {

    private $model = null;

    private $if_not_exists = null;

    private $engine = null;

    private $name = null;

    private $columns = array();

    public function __construct($model) {

        $this->model = $model;

    }

    final public function ifNotExists($data) {
    
        $this->if_not_exists = $data;
    
        return $this;
    
    }
    
    final public function engine($data) {
    
        $this->engine = $data;
    
        return $this;
    
    }
    
    final public function name($data) {
    
        $this->name = $data;
    
        return $this;
    
    }
    
    final public function columns($data) {
    
        $this->columns = $data;
    
        return $this;
    
    }

    public function getQuery() {
        
        if ( is_null($this->name) OR empty($this->columns)) throw new DatabaseException('Invalid parameters for database->create',1027);

        $if_not_exists = is_null($this->if_not_exists) ? null : " IF NOT EXISTS";

        $engine = is_null($this->engine) ? null : ' ENGINE '.$this->engine;

        switch ($this->model) {

            case 'MYSQLI':
            case 'MYSQL_PDO':

                $table_pattern = "`*_DBPREFIX_*%s`";

                $table = sprintf($table_pattern, trim($this->name));

                $query_pattern = "CREATE TABLE%s %s (%s)%s";

                $query = sprintf($query_pattern, $if_not_exists, $table, implode(', ',$this->columns), $engine);

                break;

            case 'POSTGRESQL':
            case 'DB2':
            case 'DBLIB_PDO':
            case 'ORACLE_PDO':
            case 'SQLITE_PDO':
            default:

                $table_pattern = "*_DBPREFIX_*%s";

                $table = sprintf($table_pattern, trim($this->name));

                $query_pattern = "CREATE TABLE%s %s (%s)";

                $query = sprintf($query_pattern, $if_not_exists, $table, implode(', ',$this->columns));

                break;
        
        }

        return $query;

    }

}
