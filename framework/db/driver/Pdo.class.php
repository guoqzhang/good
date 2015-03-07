<?php
namespace framework\db\driver;
use framework\db;

/**
 * PDO数据库驱动 
 */
class Pdo extends Db {

    protected $PDOStatement = null;
    private   $table        = '';

    /**
     * 架构函数 读取数据库配置信息
     * @access public
     * @param array $config 数据库配置数组
     */
    public function __construct($config=''){
        if(!empty($config)) {
            $this->config   =   $config;
            if(empty($this->config['params'])) {
                $this->config['params'] =   array();
            }
            $this->dbType = $this->_getDsnType($config['dsn']);            
        }

    }

    /**
     * 连接数据库方法
     * @access public
     */
    public function connect($config='',$linkNum=0) {
        if ( !isset($this->linkID[$linkNum]) ) {
            if(empty($config))  $config =   $this->config;
            if($this->pconnect) {
                $config['params'][\PDO::ATTR_PERSISTENT] = true;
            }
            if(version_compare(PHP_VERSION,'5.3.6','<=')){ //禁用模拟预处理语句
                $config['params'][\PDO::ATTR_EMULATE_PREPARES]  =   false;
            }
            try{
                $this->linkID[$linkNum] = new \PDO( $config['dsn'], $config['username'], $config['password'],$config['params']);
            }catch (\PDOException $e) {
                E($e -> getMessage(), $e->getCode(), $e->getPrevious() );
            }

            // 因为PDO的连接切换可能导致数据库类型不同，因此重新获取下当前的数据库类型
            $this->dbType = $this->_getDsnType($config['dsn']);
            if(in_array($this->dbType,array('MSSQL','ORACLE','IBASE','OCI'))) {
                // 由于PDO对于以上的数据库支持不够完美，所以屏蔽了 如果仍然希望使用PDO 可以注释下面一行代码
                E('由于目前PDO暂时不能完美支持'.$this->dbType.' 请使用官方的'.$this->dbType.'驱动');
            }
            $this->linkID[$linkNum]->exec('SET NAMES '.$config['charset']);
        }
        return $this->linkID[$linkNum];
    }

    /**
     * 释放查询结果
     * @access public
     */
    public function free() {
        $this->PDOStatement = null;
    }

    /**
     * 执行查询 返回数据集 默认读库
     * @access public
     * @param string $str  sql指令
     * @param array $bind 参数绑定
     * @return mixed
     */
    public function query($str,$bind=array()) {
        $this->initConnect(false);
        if ( !$this->_linkID ) return false;
        $this->queryStr = $str;
        if(!empty($bind)) {
            $this->queryStr     .=   '[ '.print_r($bind,true).' ]';
        }        
        //释放前次的查询结果
        if ( !empty($this->PDOStatement) ) $this->free();
        // 记录开始执行时间
        G('queryStartTime');
        $this->PDOStatement = $this->_linkID->prepare($str);
        if(false === $this->PDOStatement)
            E($this->error());
        // 参数绑定
        $result =   $this->PDOStatement->execute($bind);
        $this->debug();
        if ( false === $result ) {
            $this->error();
            return false;
        } else {
            return $this->getAll();
        }
    }

    /**
     * 执行语句   默认写库
     * @access public
     * @param string $str  sql指令
     * @param array $bind 参数绑定
     * @return integer
     */
    public function execute($str,$bind=array()) {
        $this->initConnect(true);
        if ( !$this->_linkID ) return false;
        $this->queryStr = $str;
        if(!empty($bind)){
            $this->queryStr     .=   '[ '.print_r($bind,true).' ]';
        }        
        $flag = false;
        //释放前次的查询结果
        if ( !empty($this->PDOStatement) ) $this->free();
        // 记录开始执行时间
        G('queryStartTime');
        $this->PDOStatement = $this->_linkID->prepare($str);
        if(false === $this->PDOStatement) {
            E($this->error());
        }
        // 参数绑定    
        $result = $this->PDOStatement->execute($bind);
        $this->debug();
        if ( false === $result) {
            $this->error();
            return false;
        } else {
            $this->numRows = $this->PDOStatement->rowCount();
            if($flag || preg_match("/^\s*(INSERT\s+INTO|REPLACE\s+INTO)\s+/i", $str)) {
                $this->lastInsID = $this->getLastInsertId();
            }
            return $this->numRows;
        }
    }

    /**
     * 获得所有的查询数据
     * @access private
     * @return array
     */
    private function getAll() {
        //返回数据集
        $result =   $this->PDOStatement->fetchAll(\PDO::FETCH_ASSOC);
        $this->numRows = count( $result );
        return $result;
    }
    /**
     * 字段和表名处理
     * @access protected
     * @param string $key
     * @return string
     */
    protected function parseKey(&$key) {
        if(!is_numeric($key) && $this->dbType=='MYSQL'){
            $key   =  trim($key);
            if(!preg_match('/[,\'\"\*\(\)`.\s]/',$key)) {
               $key = '`'.$key.'`';
            }
            return $key;            
        }else{
            return parent::parseKey($key);
        }

    }

    /**
     * 关闭数据库
     * @access public
     */
    public function close() {
        $this->_linkID = null;
    }

    /**
     * 数据库错误信息
     * 并显示当前的SQL语句
     * @access public
     * @return string
     */
    public function error() {
        if($this->PDOStatement) {
            $error = $this->PDOStatement->errorInfo();
            $this->error = $error[1].':'.$error[2];
        }else{
            $this->error = '';
        }
        if('' != $this->queryStr){
            $this->error .= "\n [ SQL语句 ] : ".$this->queryStr;
        }
        //trace($this->error,'','ERR');
        return $this->error;
    }

    /**
     * SQL指令安全过滤
     * @access public
     * @param string $str  SQL指令
     * @return string
     */
    public function escapeString($str) {
         switch($this->dbType) {
            case 'MYSQL':
                return addslashes($str);
            case 'PGSQL':                
            case 'IBASE':                
            case 'SQLITE':
            case 'ORACLE':
            case 'OCI':
                return str_ireplace("'", "''", $str);
        }
    }

    /**
     * value分析
     * @access protected
     * @param mixed $value
     * @return string
     */
    protected function parseValue($value) {
        if(is_string($value)) {
            $value =  strpos($value,':') === 0 ? $this->escapeString($value) : '\''.$this->escapeString($value).'\'';
        }elseif(isset($value[0]) && is_string($value[0]) && strtolower($value[0]) == 'exp'){
            $value =  $this->escapeString($value[1]);
        }elseif(is_array($value)) {
            $value =  array_map(array($this, 'parseValue'),$value);
        }elseif(is_bool($value)){
            $value =  $value ? '1' : '0';
        }elseif(is_null($value)){
            $value =  'null';
        }
        return $value;
    }

    /**
     * 获取最后插入id
     * @access public
     * @return integer
     */
    public function getLastInsertId() {
         switch($this->dbType) {
            case 'MYSQL':
                return $this->_linkID->lastInsertId();
        }
    }
}