<?php
namespace framework;
/**
 * 数据库中间层实现类
 */
class Db {
    // 数据库类型
    protected $dbType     = null;
    // 是否自动释放查询结果
    protected $autoFree   = false;
    // 是否使用永久连接
    protected $pconnect   = false;
    // 当前SQL指令
    protected $queryStr   = '';
    // 最后插入ID
    protected $lastInsID  = null;
    // 返回或者影响记录数
    protected $numRows    = 0;
    // 错误信息
    protected $error      = '';
    // 数据库连接ID 支持多个连接
    protected $linkID     = array();
    // 当前连接ID
    protected $_linkID    = null;
    // 当前查询ID
    protected $queryID    = null;
    // 数据库连接参数配置
    protected $config     = '';
    // 参数绑定
    protected $bind       = array();

    /**
     * 取得数据库类实例
     * @static
     * @access public
     * @return mixed 返回数据库驱动类
     */
    public static function getInstance($db_config='') {
		static $_instance	=	array();
		//生成唯一资源符
		$guid	=	to_guid_string($db_config);
		if(!isset($_instance[$guid])){
			$obj	=	new Db();
			$_instance[$guid]	=	$obj->factory($db_config);
		}
		return $_instance[$guid];
    }

    /**
     * 加载数据库 支持配置文件或者 DSN
     * @access public
     * @param mixed $db_config 数据库配置信息
     * @return string
     */
    public function factory($db_config='') {
        // 读取数据库配置
        $db_config = $this->parseConfig($db_config);
        if(empty($db_config['dbms']))
            E(L('_NO_DB_CONFIG_'));
        // 数据库类型
        if(strpos($db_config['dbms'],'\\')){
            $class  =   $db_config['dbms'];
        }else{
            $dbType =   ucwords(strtolower($db_config['dbms']));
            $class  =   'framework\\db\\driver\\'. $dbType;            
        }
        // 检查驱动类
        if(class_exists($class)) {
            $db = new $class($db_config);
        }else {
            // 类没有定义
            E(L('_NO_DB_DRIVER_').': ' . $class);
        }
        return $db;
    }

    /**
     * 根据DSN获取数据库类型 返回大写
     * @access protected
     * @param string $dsn  dsn字符串
     * @return string
     */
    protected function _getDsnType($dsn) {
        $match  =  explode(':',$dsn);
        $dbType = strtoupper(trim($match[0]));
        return $dbType;
    }

    /**
     * 分析数据库配置信息，支持数组和DSN
     * @access private
     * @param mixed $db_config 数据库配置信息
     * @return string
     */
    private function parseConfig($db_config='') {
        if ( !empty($db_config) && is_string($db_config)) {
            // 如果DSN字符串则进行解析
            $db_config = $this->parseDSN($db_config);
        }elseif(is_array($db_config)) { // 数组配置
             $db_config =   array_change_key_case($db_config);
             $db_config = array(
                  'dbms'      =>  $db_config['db_type'],
                  'username'  =>  $db_config['db_user'],
                  'password'  =>  $db_config['db_pwd'],
                  'hostname'  =>  $db_config['db_host'],
                  'hostport'  =>  $db_config['db_port'],
                  'database'  =>  $db_config['db_name'],
                  'dsn'       =>  isset($db_config['db_dsn'])?$db_config['db_dsn']:'',
                  'params'    =>  isset($db_config['db_params'])?$db_config['db_params']:array(),
                  'charset'   =>  isset($db_config['db_charset'])?$db_config['db_charset']:'utf8',
             );
        }
        return $db_config;
    }

    /**
     * 初始化数据库连接
     * @access protected
     * @param boolean $master 主服务器
     * @return void
     */
    protected function initConnect($master=true) {
        if(1 == C('DB_DEPLOY_TYPE'))
            // 采用分布式数据库
            $this->_linkID = $this->multiConnect($master);
        else
            // 默认单数据库
            if ( !$this->_linkID ) $this->_linkID = $this->connect();
    }

    /**
     * 连接分布式服务器
     * @access protected
     * @param boolean $master 主服务器
     * @return void
     */
    protected function multiConnect($master=false) {
        foreach ($this->config as $key=>$val){
            $_config[$key]      =   explode(',',$val);
        }        
        // 数据库读写是否分离
        if(C('DB_RW_SEPARATE')){
            // 主从式采用读写分离
            if($master)
                // 主服务器写入
                $r  =   floor(mt_rand(0,C('DB_MASTER_NUM')-1));
            else{
                if(is_numeric(C('DB_SLAVE_NO'))) {// 指定服务器读
                    $r = C('DB_SLAVE_NO');
                }else{
                    // 读操作连接从服务器
                    $r = floor(mt_rand(C('DB_MASTER_NUM'),count($_config['hostname'])-1));   // 每次随机连接的数据库
                }
            }
        }else{
            // 读写操作不区分服务器
            $r = floor(mt_rand(0,count($_config['hostname'])-1));   // 每次随机连接的数据库
        }
        $db_config = array(
            'username'  =>  isset($_config['username'][$r])?$_config['username'][$r]:$_config['username'][0],
            'password'  =>  isset($_config['password'][$r])?$_config['password'][$r]:$_config['password'][0],
            'hostname'  =>  isset($_config['hostname'][$r])?$_config['hostname'][$r]:$_config['hostname'][0],
            'hostport'  =>  isset($_config['hostport'][$r])?$_config['hostport'][$r]:$_config['hostport'][0],
            'database'  =>  isset($_config['database'][$r])?$_config['database'][$r]:$_config['database'][0],
            'dsn'       =>  isset($_config['dsn'][$r])?$_config['dsn'][$r]:$_config['dsn'][0],
            'params'    =>  isset($_config['params'][$r])?$_config['params'][$r]:$_config['params'][0],
            'charset'   =>  isset($_config['charset'][$r])?$_config['charset'][$r]:$_config['charset'][0],            
        );
        return $this->connect($db_config,$r);
    }

    /**
     * DSN解析
     * 格式： mysql://username:passwd@localhost:3306/DbName#charset
     * @static
     * @access public
     * @param string $dsnStr
     * @return array
     */
    public function parseDSN($dsnStr) {
        if( empty($dsnStr) ){return false;}
        $info = parse_url($dsnStr);
        if($info['scheme']){
            $dsn = array(
            'dbms'      =>  $info['scheme'],
            'username'  =>  isset($info['user']) ? $info['user'] : '',
            'password'  =>  isset($info['pass']) ? $info['pass'] : '',
            'hostname'  =>  isset($info['host']) ? $info['host'] : '',
            'hostport'  =>  isset($info['port']) ? $info['port'] : '',
            'database'  =>  isset($info['path']) ? substr($info['path'],1) : '',
            'charset'   =>  isset($info['fragment'])?$info['fragment']:'utf8',
            );
        }else {
            preg_match('/^(.*?)\:\/\/(.*?)\:(.*?)\@(.*?)\:([0-9]{1, 6})\/(.*?)$/',trim($dsnStr),$matches);
            $dsn = array (
            'dbms'      =>  $matches[1],
            'username'  =>  $matches[2],
            'password'  =>  $matches[3],
            'hostname'  =>  $matches[4],
            'hostport'  =>  $matches[5],
            'database'  =>  $matches[6]
            );
        }
        $dsn['dsn'] =  ''; // 兼容配置信息数组
        return $dsn;
     }

	 //insert update delete replace 全部执行该方案,主库
	 public function write($sql ,$data ) {
		return $this->execute($sql,$data);
	 }
     //获取多行记录,从库
	 public function read($sql,$data) {
		$result     =   $this->query($sql,$data);
        return $result;
	 }

    /**
     * 数据库调试 记录当前SQL
     * @access protected
     */
    protected function debug() {
		if (C('DB_SQL_LOG')) {
			// 记录操作结束时间
			G('queryEndTime');
			//记录log
			 Log::record('SQL:'.$this->queryStr.' [ RunTime:'.G('queryStartTime','queryEndTime',6).'s ]');
		}
    }
    /**
     * 解析参数构造完整的sql及query值
     * @access protected
     * @param array $data
     * @return string
     */
    public function parseParam($data) {
		if(empty($data ))  return ;
        foreach ($data as $key=>$val) {
             $set[]					=   $this->parseKey($key)." = :{$key}";
			 $bindParam[':'.$key]	=   $this->parseValue($val);
        }
		$str  = implode(',',$set);
        return array(
			'str'	=> $str,
			'param' => $bindParam
		);
    }
    /**
     * 字段名分析
     * @access protected
     * @param string $key
     * @return string
     */
    protected function parseKey(&$key) {
        return $key;
    }
    
    /**
     * value分析
     * @access protected
     * @param mixed $value
     * @return string
     */
    protected function parseValue($value) {
        if(is_string($value)) {
            $value =  '\''.$this->escapeString($value).'\'';
        }elseif(is_bool($value)){
            $value =  $value ? '1' : '0';
        }elseif(is_null($value)){
            $value =  'null';
        }
        return $value;
    }
    /**
     * 获取最近插入的ID
     * @access public
     * @return string
     */
    public function getLastInsID() {
        return $this->lastInsID;
    }

    /**
     * 获取最近的错误信息
     * @access public
     * @return string
     */
    public function getError() {
        return $this->error;
    }

    /**
     * SQL指令安全过滤
     * @access public
     * @param string $str  SQL字符串
     * @return string
     */
    public function escapeString($str) {
        return addslashes($str);
    }

   /**
     * 析构方法
     * @access public
     */
    public function __destruct() {
        // 释放查询
        if ($this->queryID){
            $this->free();
        }
        // 关闭连接
        $this->close();
    }

    // 关闭数据库 由驱动类定义
    public function close(){}
}