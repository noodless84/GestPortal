<?php namespace Comodojo\Database\QueryBuilder;

use \Comodojo\Exception\DatabaseException;
use \Exception;

/**
 * UPDATE query builder
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

class QueryUpdate {

    private $model = null;

    private $table = null;

    private $where = null;

    private $values_array = array();

    private $keys_array = array();

    public function __construct($model) {

        $this->model = $model;

    }

    final public function table($data) {

        $this->table = $data;

        return $this;

    }

    final public function where($data) {

        $this->where = $data;

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

        if ( is_null($this->table) OR empty($this->keys_array) OR empty($this->values_array) ) throw new DatabaseException('Invalid parameters for database->update', 1024);

        if ( sizeof($this->values_array) != 1 ) throw new DatabaseException('Cannot update multiple values at a time');

        if ( sizeof($this->keys_array) != sizeof($this->values_array[0]) ) throw new DatabaseException('Keys and values are of different sizes',1025);

        $query_pattern = "UPDATE %s SET %s%s";

        $query_content_array = array();

        foreach ($this->keys_array as $position => $key) array_push($query_content_array, $key.'='.$this->values_array[0][$position]);

        $where = is_null($this->where) ? null : " ".$this->where;

        $query = sprintf($query_pattern, $this->table, implode(',',$query_content_array), $where);

        return $query;

    }

}
