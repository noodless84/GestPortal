<?php namespace Comodojo\Database;

use \Comodojo\Exception\DatabaseException;
use \Exception;

/**
 * Enhanced database class for comodojo.
 *
 * It extends the base class 'Database' with a cross-database query builder.
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

class EnhancedDatabase extends Database {

    /**
     * Database table to manipulate
     *
     * @var string
     */
    private $table = null;

    /**
     * Default table prefix
     *
     * @var string
     */
    private $table_prefix = null;

    /**
     * Use SELECT DISTINCT instead of SELECT
     *
     * @var bool
     */
    private $distinct = false;

    /**
     * Keys, imploded as a string
     *
     * @var string
     */
    private $keys = null;

    /**
     * Keys, in array form
     *
     * @var array
     */
    private $keys_array = array();

    /**
     * Values, imploded as a string
     *
     * @var string
     */
    private $values = null;

    /**
     * Values, in array form
     *
     * @var array
     */
    private $values_array = array();

    /**
     * Where conditions
     *
     * @var string
     */
    private $where = null;

    /**
     * Table joins, if any
     *
     * @var string
     */
    private $join = null;

    /**
     * Using clause
     *
     * @var string
     */
    private $using = null;

    /**
     * On clause
     *
     * @var string
     */
    private $on = null;

    /**
     * Orderby clause
     *
     * @var string
     */
    private $order_by = null;

    /**
     * Groupby clause
     *
     * @var string
     */
    private $group_by = null;

    /**
     * Having clause
     *
     * @var string
     */
    private $having = null;

    /**
     * Columns definition for create table method
     *
     * @var string
     */
    private $columns = array();

    private $auto_clean = false;

    static private $supported_query_types = array('GET','STORE','UPDATE','DELETE','TRUNCATE','CREATE','DROP'/*,'ALTER'*/);

    /**
     * If $mode == true, builder will reset itself after each build
     *
     * @param   bool    $mode
     *
     * @return  Object  $this
     */
    final public function autoClean($mode=true) {

        $this->auto_clean = filter_var($mode, FILTER_VALIDATE_BOOLEAN);

        return $this;

    }

    /**
     * Return the current query as a string
     *
     * @return  string
     */
    final public function getQuery($query, $parameters=array()) {

        try {
            
            $query = $this->buildQuery($query, $parameters);

            $query = str_replace("*_DBPREFIX_*", $this->table_prefix, $query);

        } catch (DatabaseException $de) {
            
            throw $de;

        }

        return $query;

    }

    /**
     * Set the database table
     *
     * @param   string  $table
     *
     * @return  Object  $this
     */
    final public function table($table) {

        $table_pattern = in_array($this->model, Array('MYSQLI','MYSQL_PDO')) ? "`*_DBPREFIX_*%s`" : "*_DBPREFIX_*%s";

        if ( empty($table) ) throw new DatabaseException('Invalid table name',1010);

        else $this->table = sprintf($table_pattern,trim($table));

        return $this;

    }

    final public function tablePrefix($prefix) {

        $this->table_prefix = empty($prefix) ? null : $prefix;

        return $this;

    }

    /**
     * Enable use of SELECT DISTINCT instead of SELECT
     *
     * @param   bool    $value
     *
     * @return  Object  $this
     */
    final public function distinct(/*$value*/) {

        $this->distinct = true; /*filter_var($value, FILTER_VALIDATE_BOOLEAN);*/

        return $this;

    }

    /**
     * Declare query keys
     *
     * Param $keys may be a string or an array of values.
     *
     * In both cases, there are two special char notation that are processed if contained in a key definition:
     *
     * - '::' defines an operation on one or more keys:
     *
     *        - $this->keys('COUNT::id') will be transformed into COUNT('id')
     *
     *        - $this->keys('CONCAT::first::, ::second') will be transformed into CONCAT(first,', ',second) 
     *
     * - '=>' defines an alias name for key:
     *
     *        - $this->keys('id=>foo') will be transformed into 'id' AS 'foo'
     *
     * Two notation can be mixed to obtain a complex expression:
     *
     *        - $this->keys('CONCAT::id::, ::position=>foo') become CONCAT(id,', ',position) AS foos
     *
     * @param   mixed   $keys
     *
     * @return  Object  $this
     */
    public function keys($keys) {

        $processed_keys = array();

        try {
            
            if ( empty($keys) ) throw new DatabaseException('Invalid key/s',1011);

            else if ( is_array($keys) ) foreach ($keys as $key) array_push($processed_keys, $this->composeKey($key));

            else array_push($processed_keys, $this->composeKey($keys));


        } catch (DatabaseException $de) {
            
            throw $de;

        }

        $this->keys = implode(',', $processed_keys);

        $this->keys_array = $processed_keys;
        
        return $this;
        
    }

    /**
     * Declare query values
     *
     * Param $values may be a string or an array of values.
     *
     * This method can be called n times to add multiple values to query.
     *
     * @param   mixed   $values
     *
     * @return  Object  $this
     */
    public function values($values) {

        $processed_values = array();

        try {
            
            //if ( empty($values) ) throw new DatabaseException('Invalid value/s',1014);

            if ( is_array($values) ) foreach ($values as $value) array_push($processed_values, $this->composeValue($value));

            else array_push($processed_values, $this->composeValue($values));


        } catch (DatabaseException $de) {
            
            throw $de;

        }

        $this->values = is_null($this->values) ? '('.implode(',', $processed_values).')' : $this->values.', ('.implode(',', $processed_values).')';

        array_push($this->values_array, $processed_values);
        
        return $this;
        
    }

    /**
     * Add first where condition
     *
     * Simple where conditions are composed defining scalar $column and $value parameters; for example:
     *
     * - $this->where('name','=', "jhon")
     *
     * - $this->where('name','LIKE', "jhon%")
     *
     * Nested where conditions can be created using nested arrays; for example:
     *
     * - $this->where(array('name','=', "jhon"), 'AND', array('lastname', '=', 'smith'))
     *
     * - $this->where('name', 'BETWEEN', array(1,10))
     *
     * This method can be also called n times to add multiple values to query.
     *
     * @param   mixed   $column
     * @param   string  $operator
     * @param   mixed   $value
     *
     * @return  Object  $this
     */
    public function where($column, $operator, $value) {
        
        try {

            $this->where = "WHERE ".$this->composeWhereCondition($column, $operator, $value);

        }
        catch (DatabaseException $de) {

            throw $de;

        }

        return $this;

    }

    /**
     * Add an AND where condition
     *
     * @param   mixed   $column
     * @param   string  $operator
     * @param   mixed   $value
     *
     * @return  Object  $this
     */
    public function andWhere($column, $operator, $value) {
        
        try {

            $this->where .= " AND ".$this->composeWhereCondition($column, $operator, $value);

        }
        catch (DatabaseException $de) {

            throw $de;

        }

        return $this;

    }

    /**
     * Add an OR where condition
     *
     * @param   mixed   $column
     * @param   string  $operator
     * @param   mixed   $value
     *
     * @return  Object  $this
     */
    public function orWhere($column, $operator, $value) {
        
        try {

            $this->where .= " OR ".$this->composeWhereCondition($column, $operator, $value);

        }
        catch (DatabaseException $de) {

            throw $de;

        }

        return $this;

    }

    /**
     * Add a join clause to the query.
     *
     * WARNING: not all databases support joins like RIGHT, NATURAL or FULL.
     * This method WILL NOT alert or throw exception in case of unsupported join,
     * this kind of check will be implemented in next versions.
     *
     * @param   string  $join_type
     * @param   string  $table
     *
     * @return  Object  $this
     */
    public function join($join_type, $table, $as=null) {
        
        $join = strtoupper($join_type);

        $join_type_list = Array('INNER','NATURAL','CROSS','LEFT','RIGHT','LEFT OUTER','RIGHT OUTER','FULL OUTER',null);

        if ( !in_array($join, $join_type_list) OR empty($table) ) throw new DatabaseException('Invalid parameters for database join',1019);

        if ( is_null($as) ) {

            $join_pattern = " %sJOIN %s";

            if ( is_null($this->join) ) $this->join = sprintf($join_pattern, $join." ", $table);

            else $this->join .= " ".sprintf($join_pattern, $join." ", $table);

        } else {

            $join_pattern = " %sJOIN %s AS %s";

            if ( is_null($this->join) ) $this->join = sprintf($join_pattern, $join." ", $table, $as);

            else $this->join .= " ".sprintf($join_pattern, $join." ", $table, $as);

        }
        
        return $this;

    }

    /**
     * Add a using clause to the query.
     *
     * $columns may be a string (signle column) or an array (multple columns)
     *
     * @param   mixed   $columns
     *
     * @return  Object  $this
     */
    public function using($columns) {

        $using_pattern = "USING (%s)";

        if (empty($columns)) throw new DatabaseException('Invalid parameters for database::using',1020);
        
        $this->using = sprintf($using_pattern, is_array($columns) ? implode(',', $columns) : $columns);
        
        return $this;

    }

    /**
     * Add a ON clause to the query.
     *
     * @param   string  $first_column
     * @param   string  $operator
     * @param   string  $second_column
     *
     * @return  Object  $this
     */
    public function on($first_column, $operator, $second_column) {

        try {

            $this->on = "ON ".$this->composeOnClause($first_column, $operator, $second_column);

        }
        catch (DatabaseException $de) {

            throw $de;

        }

        return $this;

    }

    /**
     * Add an AND ON clause to the query.
     *
     * @param   string  $first_column
     * @param   string  $operator
     * @param   string  $second_column
     *
     * @return  Object  $this
     */
    public function andOn($first_column, $operator, $second_column) {

        try {

            $this->on .= " AND ".$this->composeOnClause($first_column, $operator, $second_column);

        }
        catch (DatabaseException $de) {

            throw $de;

        }

        return $this;

    }

    /**
     * Add a OR ON clause to the query.
     *
     * @param   string  $first_column
     * @param   string  $operator
     * @param   string  $second_column
     *
     * @return  Object  $this
     */
    public function orOn($first_column, $operator, $second_column) {

        try {

            $this->on .= " OR ".$this->composeOnClause($first_column, $operator, $second_column);

        }
        catch (DatabaseException $de) {

            throw $de;

        }

        return $this;

    }

    /**
     * Order by column or a group of columns
     *
     * @param   mixed   $columns
     * @param   mixed   $directions
     *
     * @return  Object  $this
     */
    public function orderBy($columns, $directions=null) {

        if ( empty($columns) ) throw new DatabaseException('Invalid order by column',1012);

        $supported_directions = array("DESC", "ASC");

        switch ($this->model) {
            
            case ("SQLITE_PDO"):

                $order_column_pattern = " %s COLLATE NOCASE%s";

                break;

            //case ("MYSQLI"):
            //case ("MYSQL_PDO"):
            //case ("POSTGRESQL"):
            //case ("DB2"):
            //case ("DBLIB_PDO"):
            //case ("ORACLE_PDO"):
            default:

                $order_column_pattern = " %s%s";

                break;

        }

        if ( is_array($columns) ) {

            $column = array();

            for ( $i=0; $i < sizeof($columns)-1; $i++ ) {
                
                if ( is_array($directions) AND @isset($directions[$i]) AND @in_array(strtoupper($directions[$i]), $supported_directions) ) $direction = ' '.strtoupper($direction[$i]);

                else $direction = '';

                array_push($column, sprintf($order_column_pattern, $columns[$i], $direction));

            }

            $this->order_by = "ORDER BY".implode(', ', $column);

        } else {

            $column = trim($columns);

            $direction = is_null($directions) ? null : ' '.strtoupper($directions);

            $this->order_by = "ORDER BY".sprintf($order_column_pattern, $column, $direction);

        }

        return $this;

    }

    /**
     * Group by column or a group of columns
     *
     * @param   mixed   $columns
     *
     * @return  Object  $this
     */
    public function groupBy($columns) {

        $group_column_pattern = "%s";

        if ( empty($columns) ) throw new DatabaseException('Invalid group by column',1013);

        elseif ( is_array($columns) ) {

            array_walk($columns, function(&$column, $key) {

                $column = sprintf($group_column_pattern, trim($column));

            });

            $this->group_by = "GROUP BY ".implode(',', $columns);

        }
        else {

            $column = trim($columns);

            $this->group_by = "GROUP BY ".sprintf($group_column_pattern, $column);

        }

        return $this;

    }

    /**
     * Set the having clause in a sql statement.
     *
     * Differently from other methods, $having_clause_or_array should contain the FULL CLAUSE.
     *
     * @param mixed $having_clauses
     *
     * @return array
     */
    public function having($having_clauses) {

        $having_column_pattern = "%s";

        if ( empty($having_clauses) ) throw new DatabaseException('Invalid having clause',1028);

        elseif ( is_array($having_clauses) ) {

            array_walk($having_clauses, function(&$column, $key) {

                $column = sprintf($having_column_pattern, trim($column));

            });

            $this->having = "HAVING ".implode(' AND ', $having_clauses);

        }
        else $this->having = "HAVING ".sprintf($having_column_pattern, trim($having_clauses));

        return $this;

    }

    public function column(\Comodojo\Database\QueryBuilder\Column $column) {

        array_push($this->columns, $column->getColumnDefinition($this->model));

        return $this;

    }

    public function get($limit=0, $offset=0, $return_raw=false) {

        try {
            
            $query = $this->buildQuery('GET', array(
                "limit" =>  $limit,
                "offset"=>  $offset
            ));

            $result = $this->query($query, $return_raw);

        } catch (DatabaseException $de) {
            
            throw $de;

        }

        return $result;

    }

    public function store($return_raw=false) {

        try {
            
            $query = $this->buildQuery('STORE');

            $result = $this->query($query, $return_raw);

        } catch (DatabaseException $de) {
            
            throw $de;

        }

        return $result;

    }

    public function update($return_raw=false) {

        try {
            
            $query = $this->buildQuery('UPDATE');

            $result = $this->query($query, $return_raw);

        } catch (DatabaseException $de) {
            
            throw $de;

        }

        return $result;

    }

    public function delete($return_raw=false) {

        try {
            
            $query = $this->buildQuery('DELETE');

            $result = $this->query($query, $return_raw);

        } catch (DatabaseException $de) {
            
            throw $de;

        }

        return $result;

    }

    public function truncate($return_raw=false) {

        try {
            
            $query = $this->buildQuery('TRUNCATE');

            $result = $this->query($query, $return_raw);

        } catch (DatabaseException $de) {
            
            throw $de;

        }

        return $result;

    }

    public function create($name, $if_not_exists=false, $engine=null, $return_raw=false) {

        if ( empty($name) ) throw new DatabaseException("Invalid or empty table name");
        
        try {
            
            $query = $this->buildQuery('CREATE', array(
                "name"          =>  $name,
                "if_not_exists" =>  $if_not_exists,
                "engine"        =>  $engine
            ));

            $result = $this->query($query, $return_raw);

        } catch (DatabaseException $de) {
            
            throw $de;

        }

        return $result;

    }

    public function drop($if_exists=false, $return_raw=false) {

        try {
            
            $query = $this->buildQuery('DROP', array(
                "if_exists" =>  $if_exists
            ));

            $result = $this->query($query, $return_raw);

        } catch (DatabaseException $de) {
            
            throw $de;

        }

        return $result;

    }

    public function query($query, $return_raw=false) {

        $query = str_replace("*_DBPREFIX_*", $this->table_prefix, $query);

        return parent::query($query, $return_raw);

    }

    /**
     * Cleanup querybuilder
     *
     * If $deep, call also Database->clean()
     *
     * @param bool $deep
     *
     * @return array
     */
    public function clean($deep=false) {
        
        $this->table = null;

        $this->distinct = false;

        $this->keys = null;

        $this->keys_array = array();

        $this->values = null;

        $this->values_array = array();

        $this->where = null;

        $this->join = null;

        $this->using = null;

        $this->on = null;

        $this->order_by = null;

        $this->group_by = null;

        $this->having = null;

        $this->columns = array();

        if ( filter_var($deep, FILTER_VALIDATE_BOOLEAN) ) {

            $this->table_prefix = null;

            parent::clean();

        }

        return $this;

    }

    public function convertDate($dateString) {

        $dateReal = strtotime($dateString);

        switch ($this->dbDataModel) {

            case 'MYSQLI':
            case 'MYSQL_PDO':
            case 'DBLIB_PDO':
            case 'POSTGRESQL':
            case 'DB2':
                
                $dateObject = date("Y-m-d", $dateReal);

                break;

            case 'ORACLE_PDO':

                $dateObject = date("d-M-y", $dateReal);

                break;

            case 'SQLITE_PDO':

                $dateObject = date("c", $dateReal);

                break;

        }

        return $dateObject;

    }

    public function convertTime($timeString) {

        return ltrim($timeString,'T');
        
    }

    /**
     * Keys composer
     *
     * @param   string  $key
     *
     * @return  string
     */
    private function composeKey($key) {

        $key_pattern = in_array($this->model, Array('MYSQLI','MYSQL_PDO')) ? "`%s`" : "%s";

        if ( !is_scalar($key) ) throw new DatabaseException("Invalid key");
        
        $key = trim($key);

        // process alias notation (=>)

        $alias_array = explode('=>', $key);

        $alias_array_size = sizeof($alias_array);

        if ( $alias_array_size == 1 ) {

            //no alias, keep key definition
            $alias = '';

        } else if ( $alias_array_size == 2 ) {

            //alias defined, splitting keys

            $key = $alias_array[0];

            $alias = ' AS '.$alias_array[1];

        } else throw new DatabaseException("Invalid key definition");
        
        // process operation notation (::)

        $operation_array = explode('::', $key);

        $operation_array_size = sizeof($operation_array);

        if ( $operation_array_size == 1 ) {

            //no operation, keep key definition
            $value = $operation_array[0];

            $key = $value == '*' ? $value : sprintf($key_pattern, $value);

        } else {

            $operation = $operation_array[0];

            unset($operation_array[0]);

            array_walk($operation_array, function(&$value, $index) {

                $value = $value == '*' ? $value : sprintf($key_pattern, $value);

            });

            $key = $operation.'('.implode(',', $operation_array).')';

        }

        return $key.$alias;

    }

    /**
     * Values composer
     *
     * @param   string  $value
     *
     * @return  string
     */
    private function composeValue($value) {

        //if ( empty($value) ) throw new DatabaseException('Invalid value',1014);

        $value_string_pattern = "'%s'";

        $value_null_pattern = 'null';

        $processed_value = null;

        if  ( is_bool($value) === true ) {

            switch ($this->model) {

                case 'MYSQLI':
                case 'MYSQL_PDO':
                case 'POSTGRESQL':
                case 'DB2':

                    $processed_value = $value ? 'TRUE' : 'FALSE';

                    break;

                case 'DBLIB_PDO':
                case 'ORACLE_PDO':
                case 'SQLITE_PDO':
                default:
                    
                    $processed_value = !$value ? 0 : 1;
                    
                    break;

            }

        }

        elseif ( is_numeric($value) ) $processed_value = $value;

        elseif ( is_null($value) ) $processed_value = $value_null_pattern;

        else {

            switch ($this->model) {
                
                case 'MYSQLI':

                    $processed_value = sprintf($value_string_pattern, $this->dbh->escape_string($value));

                    break;
                
                case 'POSTGRESQL':

                    $processed_value = sprintf($value_string_pattern, pg_escape_string($value));

                    break;
                
                case 'DB2':

                    $processed_value = sprintf($value_string_pattern, db2_escape_string($value));

                    break;
                
                case 'MYSQL_PDO':
                case 'ORACLE_PDO':
                case 'SQLITE_PDO':
                case 'DBLIB_PDO':

                    $processed_value = $this->dbh->quote($value);

                    $processed_value = $processed_value === false ? sprintf($value_string_pattern, $value) : $processed_value;

                    break;
                
                default:

                    $processed_value = sprintf($value_string_pattern, $value);

                    break;

            }

        }

        return $processed_value;

    }

    /**
     * Where clause composer
     *
     * @param   mixed   $column
     * @param   string  $operator
     * @param   mixed   $value
     *
     * @return  string
     */
    private function composeWhereCondition($column, $operator, $value) {

        $to_return = null;

        $operator = strtoupper($operator);

        if ( is_array($column) AND is_array($value) ) {

            $clause_pattern = "(%s %s %s)";

            if ( !in_array($operator, Array('AND','OR')) ) throw new DatabaseException('Invalid syntax for a where clause',1017);
            
            if ( sizeof($column) != 3 OR sizeof($value) != 3 ) throw new DatabaseException('Invalid syntax for a where clause',1017);

            try {

                $processed_column = $this->composeWhereCondition($column[0],$column[1],$column[2]);

                $processed_value = $this->composeWhereCondition($value[0],$value[1],$value[2]);

            }
            catch (DatabaseException $e) {

                throw $e;

            }

            $to_return = sprintf($clause_pattern, $processed_column, $operator, $processed_value);

        } elseif ( is_scalar($column) AND is_array($value) ) {

            switch($operator) {

                case 'IN':

                    $clause_pattern = in_array($this->model, Array('MYSQLI','MYSQL_PDO')) ? "`%s` IN (%s)" : "%s IN (%s)";

                    array_walk($value, function(&$keyvalue, $key) {

                        if  ( is_bool($keyvalue) === true ) {

                            switch ($this->model) {

                                case 'MYSQLI':
                                case 'MYSQL_PDO':
                                case 'POSTGRESQL':
                                case 'DB2':

                                    $keyvalue = $keyvalue ? 'TRUE' : 'FALSE';

                                    break;

                                case 'DBLIB_PDO':
                                case 'ORACLE_PDO':
                                case 'SQLITE_PDO':
                                default:

                                    $keyvalue = $keyvalue ? 1 : 0;

                                    break;

                            }

                        }

                        elseif ( is_numeric($keyvalue) ) $keyvalue = $keyvalue;

                        elseif ( is_null($keyvalue) ) $keyvalue = "NULL";

                        else $keyvalue = "'".$keyvalue."'";

                    });

                    $processed_value = implode(",", $value);

                    $to_return = sprintf($clause_pattern, $column, $processed_value);

                    break;

                case 'BETWEEN':

                    $clause_pattern = in_array($this->model, Array('MYSQLI','MYSQL_PDO')) ? "`%s` BETWEEN %s AND %s" : "%s BETWEEN %s AND %s";

                    $to_return = sprintf($clause_pattern, $column, intval($value[0]), intval($value[1]));

                    break;

                case 'NOT IN':
                case 'NOTIN':

                    $clause_pattern = in_array($this->model, Array('MYSQLI','MYSQL_PDO')) ? "`%s` NOT IN (%s)" : "%s NOT IN (%s)";

                    $processed_value = "'".implode("','", $value)."'";

                    $to_return = sprintf($clause_pattern, $column, $processed_value);

                    break;

                case 'NOT BETWEEN':
                case 'NOTBETWEEN':

                    $clause_pattern = in_array($this->model, Array('MYSQLI','MYSQL_PDO')) ? "`%s` NOT BETWEEN %s AND %s" : "%s NOT BETWEEN %s AND %s";

                    $to_return = sprintf($clause_pattern, $column, intval($value[0]), intval($value[1]));

                    break;

                default:
                    
                    throw new DatabaseException('Invalid syntax for a where clause',1017);

                    break;

            }

        } elseif ( is_scalar($column) AND ( is_scalar($value) OR is_null($value) ) ) {
            
            $clause_pattern = in_array($this->model, Array('MYSQLI','MYSQL_PDO')) ? "`%s` %s %s" : "%s %s %s";

            if ($operator == 'IS' OR $operator == 'IS NOT' OR $operator == 'ISNOT' ) {

                $processed_column = $column;

                $processed_operator = $operator == 'IS' ? $operator : 'IS NOT';

                $processed_value = ( is_null($value) OR $value == 'NULL' ) ? 'NULL' : 'NOT NULL';

            }
            elseif ( $operator == 'LIKE' OR $operator == 'NOT LIKE' OR $operator == 'NOTLIKE' ) {

                $processed_column = $column;

                $processed_operator = $operator == 'LIKE' ? $operator : 'NOT LIKE';

                $processed_value = "'".$value."'";

            }
            else {

                $processed_column = $column;

                $processed_operator = $operator;

                if  ( is_bool($value) === true ) {

                    switch ($this->model) {

                        case 'MYSQLI':
                        case 'MYSQL_PDO':
                        case 'POSTGRESQL':
                        case 'DB2':

                            $processed_value = $value ? 'TRUE' : 'FALSE';

                            break;

                        case 'DBLIB_PDO':
                        case 'ORACLE_PDO':
                        case 'SQLITE_PDO':
                        default:

                            $processed_value = $value ? 1 : 0;

                            break;

                    }

                }

                elseif ( is_numeric($value) ) $processed_value = $value;

                elseif ( is_null($value) ) $processed_value = "NULL";

                else $processed_value = "'".$value."'";

            }

            $to_return = sprintf($clause_pattern, $processed_column, $processed_operator, $processed_value);            

        }
        else throw new DatabaseException('Invalid syntax for a where clause',1017);

        return $to_return;

    }

    private function composeOnClause($first_column, $operator, $second_column) {

        $valid_operators = Array('=','!=','>','>=','<','<=','<>');

        $on_pattern = "%s%s%s";

        if ( !in_array($operator, $valid_operators) ) throw new DatabaseException('Invalid syntax for a on clause', 1021);
        
        return sprintf($on_pattern, $first_column, $operator, $second_column);

    }

    private function buildQuery($query, $parameters = array()) {

        if ( empty($query) ) throw new DatabaseException("Invalid query type");

        $query = strtoupper($query);

        if ( !in_array($query, self::$supported_query_types) ) throw new DatabaseException('Unsupported query type for buidler');
        
        try {
        
            switch ($query) {

                case 'GET':
                    
                    $builder = new \Comodojo\Database\QueryBuilder\QueryGet($this->model);

                    if ( array_key_exists('limit', $parameters) ) $builder->limit($parameters['limit']);
                    if ( array_key_exists('offset', $parameters) ) $builder->offset($parameters['offset']); 
                    
                    $builder->table($this->table)
                        ->keys($this->keys)
                        ->distinct($this->distinct)
                        ->join($this->join)
                        ->using($this->using)
                        ->on($this->on)
                        ->where($this->where)
                        ->groupBy($this->group_by)
                        ->having($this->having)
                        ->orderBy($this->order_by);

                    $composed_query = $builder->getQuery();

                    break;

                case 'STORE':
                    
                    $builder = new \Comodojo\Database\QueryBuilder\QueryStore($this->model);

                    $builder->table($this->table)
                        ->keys($this->keys)
                        ->values($this->values)
                        ->keysArray($this->keys_array)
                        ->valuesArray($this->values_array);

                    $composed_query = $builder->getQuery();

                    break;

                case 'UPDATE':
                    
                    $builder = new \Comodojo\Database\QueryBuilder\QueryUpdate($this->model);

                    $builder->table($this->table)
                        ->where($this->where)
                        ->keysArray($this->keys_array)
                        ->valuesArray($this->values_array);

                    $composed_query = $builder->getQuery();

                    break;
                
                case 'DELETE':
                    
                    $builder = new \Comodojo\Database\QueryBuilder\QueryDelete($this->model);

                    $builder->table($this->table)->where($this->where);

                    $composed_query = $builder->getQuery();

                    break;

                case 'TRUNCATE':
                    
                    $builder = new \Comodojo\Database\QueryBuilder\QueryTruncate($this->model);

                    $builder->table($this->table);

                    $composed_query = $builder->getQuery();

                    break;

                case 'CREATE':

                    $builder = new \Comodojo\Database\QueryBuilder\QueryCreate($this->model);

                    if ( array_key_exists('name', $parameters) ) $builder->name($parameters['name']);
                    if ( array_key_exists('if_not_exists', $parameters) ) $builder->ifNotExists($parameters['if_not_exists']);
                    if ( array_key_exists('engine', $parameters) ) $builder->engine($parameters['engine']);

                    $builder->columns($this->columns);

                    $composed_query = $builder->getQuery();

                    break;

                case 'DROP':

                    $builder = new \Comodojo\Database\QueryBuilder\QueryDrop($this->model);

                    if ( array_key_exists('if_exists', $parameters) ) $builder->ifExists($parameters['if_exists']);
                    
                    $builder->table($this->table);

                    $composed_query = $builder->getQuery();

                    break;

                default:

                    throw new DatabaseException('Invalid query for querybuilder');

                    break;
            }

        } catch (DatabaseException $de) {
            
            throw $de;

        }

        if ( $this->auto_clean ) $this->clean();

        return $composed_query;

    } 

}
