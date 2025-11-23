package qesyPgSql

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Db 指针
var Db *pgxpool.Pool

// OpenLog 是否记录日志
var (
	OpenLog           int           = 0
	MaxConns          int32         = 100
	MinConns          int32         = 20
	MaxConnIdleTime   time.Duration = 15 * time.Minute
	MaxConnLifetime   time.Duration = 2 * time.Hour
	HealthCheckPeriod time.Duration = 1 * time.Minute
)

// Model 结构
type Model struct {
	Cond        interface{}
	Insert      map[string]string
	InsertArr   []map[string]string
	Update      map[string]string
	Field       string
	Table       string
	Index       string
	Limit       interface{}
	Sort        string
	GroupBy     string
	IsDeug      int
	ParamsIndex int
	Tx          pgx.Tx
	Ctx         context.Context
	Scan        []interface{}
}

// Connect  is a method with a sql.
func Connect(Host, Port, UserName, Password, DbName string) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", UserName, Password, Host, Port, DbName)
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("Unable to parse config: %v\n", err)
	}

	// 连接池配置
	config.MaxConns = MaxConns                   // 最大连接数
	config.MinConns = MinConns                   // 最小连接数
	config.MaxConnIdleTime = MaxConnIdleTime     // 连接最大空闲时间
	config.MaxConnLifetime = MaxConnLifetime     // 连接最大存活时间
	config.HealthCheckPeriod = HealthCheckPeriod // 健康检查周期

	Db, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}
}

// Begin 开始事务
func (m *Model) Begin() error {
	var Err error = nil
	m.Tx, Err = Db.Begin(m.Ctx)
	return Err
}

// Rollback 事务回滚
func (m *Model) Rollback() error {
	return m.Tx.Rollback(m.Ctx)
}

// Commit 事务提交
func (m *Model) Commit() error {
	return m.Tx.Commit(m.Ctx)
}

// ExecSelectIndex 返回一个MAP
func (m *Model) ExecSelectIndex() (map[string]map[string]string, error) {
	resultsSlice, err := m.execSelect()
	retArr := map[string]map[string]string{}
	for _, v := range resultsSlice {
		if v[m.Index] == "" {
			continue
		}
		retArr[v[m.Index]] = v
	}
	m.Clean()
	return retArr, err
}

// Query 查询SQL,返回一个 切片MAP;
// SqlStr : SQL语句
func (m *Model) Query(sqlStr string) ([]map[string]string, error) {
	ret, err := m.query(sqlStr)
	m.Clean()
	return ret, err
}

// ExecSelect 执行查询 返回一个 切片MAP
func (m *Model) ExecSelect() ([]map[string]string, error) {
	ret, err := m.execSelect()
	m.Clean()
	return ret, err
}

// ExecSelect 拼装SQL语句
func (m *Model) execSelect() ([]map[string]string, error) {
	field := m.getSQLField()
	cond := m.getSQLCond()
	groupby := m.getGroupBy()
	sort := m.getSort()
	limit := m.getSQLLimite()
	sqlStr := `SELECT ` + field + ` FROM ` + m.Table + cond + groupby + sort + limit + `;`
	return m.query(sqlStr)
}

// ExecSelectOne 只查询一条
func (m *Model) ExecSelectOne() (map[string]string, error) {
	m.SetLimit([2]int{0, 1})
	resultsSlice, err := m.ExecSelect()
	if len(resultsSlice) == 0 {
		return map[string]string{}, err
	}
	return resultsSlice[0], nil
}

// ExecUpdate 修改
func (m *Model) ExecUpdate() error {
	updateStr := m.getSQLUpdate()
	condStr := m.getSQLCond()
	sqlStr := `UPDATE ` + m.Table + ` SET ` + updateStr + condStr + `;`
	m.Debug(sqlStr)
	var err error
	if m.Tx == nil {
		_, err = Db.Exec(
			m.Ctx,
			sqlStr,
			m.Scan...,
		)
	} else {
		_, err = m.Tx.Exec(
			m.Ctx,
			sqlStr,
			m.Scan...,
		)
	}
	m.Clean()
	return err
}

// ExecInsert 添加
func (m *Model) ExecInsert(PrimaryKey string) (string, error) {
	insert := m.getSQLInsert()
	sqlStr := "INSERT INTO " + m.Table + " " + insert + " RETURNING " + PrimaryKey + ";"
	m.Debug(sqlStr)
	var err error
	var id string
	if m.Tx == nil {
		err = Db.QueryRow(
			m.Ctx,
			sqlStr,
			m.Scan...,
		).Scan(&id)
	} else {
		err = m.Tx.QueryRow(
			m.Ctx,
			sqlStr,
			m.Scan...,
		).Scan(&id)
	}
	m.Clean()
	return id, err
}

// ExecInsertBatch 批量添加 （预计要删除）
func (m *Model) ExecInsertBatch() ([]string, error) {
	insert := m.getSQLInsertArr()
	sqlStr := `INSERT INTO ` + m.Table + ` ` + insert + `;`
	m.Debug(sqlStr)
	var err error
	var ids []string
	var rows pgx.Rows

	if m.Tx == nil {
		rows, err = Db.Query(
			m.Ctx,
			sqlStr,
			m.Scan...,
		)
	} else {
		rows, err = m.Tx.Query(
			m.Ctx,
			sqlStr,
			m.Scan...,
		)
	}
	for rows.Next() {
		var id string
		rows.Scan(&id)
		ids = append(ids, id)
	}
	m.Clean()
	return ids, err
}

// ExecReplace 替换 （PgSql暂时不做）
// func (m *Model) ExecReplace() (sql.Result, error) {
// 	insert := m.getSQLInsert()
// 	sqlStr := "REPLACE INTO " + m.Table + " " + insert + ";"
// 	m.Debug(sqlStr)
// 	var err error
// 	var stmt *sql.Stmt
// 	if m.Tx == nil {
// 		stmt, err = Db.Prepare(sqlStr)
// 	} else {
// 		stmt, err = m.Tx.Prepare(sqlStr)
// 	}
// 	if err != nil {
// 		logRecord("ERR:" + err.Error() + "SQL:" + sqlStr)
// 		return nil, err
// 	}
// 	defer stmt.Close()
// 	result, err := stmt.Exec(m.Scan...)
// 	m.Clean()
// 	return result, err
// }

// ExecReplace 替换 （PgSql暂时不做）
// func (m *Model) ExecReplaceBatch() (sql.Result, error) {
// 	insert := m.getSQLInsertArr()
// 	sqlStr := "REPLACE INTO " + m.Table + " " + insert + ";"
// 	m.Debug(sqlStr)
// 	var err error
// 	var stmt *sql.Stmt
// 	if m.Tx == nil {
// 		stmt, err = Db.Prepare(sqlStr)
// 	} else {
// 		stmt, err = m.Tx.Prepare(sqlStr)
// 	}
// 	if err != nil {
// 		logRecord("ERR:" + err.Error() + "SQL:" + sqlStr)
// 		return nil, err
// 	}
// 	defer stmt.Close()
// 	result, err := stmt.Exec(m.Scan...)
// 	m.Clean()
// 	return result, err
// }

// ExecDelete 删除
func (m *Model) ExecDelete() error {
	condStr := m.getSQLCond()
	sqlStr := `DELETE FROM ` + m.Table + condStr + `;`
	m.Debug(sqlStr)
	var err error
	if m.Tx == nil {
		_, err = Db.Exec(
			m.Ctx,
			sqlStr,
			m.Scan...,
		)
	} else {
		_, err = m.Tx.Exec(
			m.Ctx,
			sqlStr,
			m.Scan...,
		)
	}
	m.Clean()
	return err
}

// Exec 执行SQL语句
func (m *Model) Exec(sqlStr string, params []interface{}) error {
	var err error
	if m.Tx == nil {
		_, err = Db.Exec(
			m.Ctx,
			sqlStr,
			params...,
		)
	} else {
		_, err = m.Tx.Exec(
			m.Ctx,
			sqlStr,
			params...,
		)
	}
	m.Clean()
	return err
}

func (m *Model) getSQLCond() string {
	if str, ok := m.Cond.(string); ok || m.Cond == nil {
		if str == `` {
			return str
		}
		return ` WHERE ` + str + ` `
	}
	var strArr []string
	if arr, ok := m.Cond.(map[string]string); ok {
		for k, v := range arr {
			m.Scan = append(m.Scan, v)
			placeholder := fmt.Sprintf("$%d", m.ParamsIndex)
			if strings.Contains(k, "LIKE") {
				strArr = append(strArr, k+" "+placeholder)
			} else if strings.Contains(k, ">") || strings.Contains(k, "<") {
				strArr = append(strArr, k+" "+placeholder)
			} else {
				strArr = append(strArr, k+"="+placeholder)
			}
			m.ParamsIndex++
		}
	} else if arr, ok := m.Cond.(map[string]interface{}); ok {
		for k, v := range arr {
			placeholder := fmt.Sprintf("$%d", m.ParamsIndex)
			if _, ok := v.(string); ok {
				m.Scan = append(m.Scan, v)
				if strings.Contains(k, "LIKE") {
					strArr = append(strArr, k+" "+placeholder)
				} else if strings.Contains(k, ">") || strings.Contains(k, "<") {
					strArr = append(strArr, k+" "+placeholder)
				} else {
					strArr = append(strArr, k+"="+placeholder)
				}

			} else if isStrArrTmp, ok := v.([]string); ok {
				if len(isStrArrTmp) == 0 {
					m.Scan = append(m.Scan, "")
					strArr = append(strArr, k+"="+placeholder)
				} else {
					SqlIn := []string{}
					for _, sv := range isStrArrTmp {
						m.Scan = append(m.Scan, sv)
						SqlIn = append(SqlIn, placeholder)
					}
					strArr = append(strArr, k+" in ("+strings.Join(SqlIn, ",")+")")
				}
			}
			m.ParamsIndex++
		}
	}
	if len(strArr) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(strArr, " && ")
}

func (m *Model) getSQLField() string {
	Field := "*"
	if m.Field != "" {
		Field = m.Field
	}
	return Field
}

func (m *Model) getSort() string {
	if m.Sort != "" {
		return " ORDER BY " + m.Sort + " "
	}
	return ""
}

func (m *Model) getGroupBy() string {
	if m.GroupBy != "" {
		return " GROUP BY " + m.GroupBy + " "
	}
	return ""
}

func (m *Model) getSQLUpdate() string {
	var strArr []string
	for k, v := range m.Update {
		placeholder := fmt.Sprintf("$%d", m.ParamsIndex)
		m.Scan = append(m.Scan, v)
		strArr = append(strArr, `"`+k+`"=`+placeholder)
		m.ParamsIndex++
	}
	return strings.Join(strArr, ",")
}

func (m *Model) getSQLInsert() string {
	var fieldArr, valueArr []string
	for k, v := range m.Insert {
		m.Scan = append(m.Scan, v)
		fieldArr = append(fieldArr, k)
		placeholder := fmt.Sprintf("$%d", m.ParamsIndex)
		valueArr = append(valueArr, placeholder)
		m.ParamsIndex++
	}
	return "(" + strings.Join(fieldArr, ",") + ") values (" + strings.Join(valueArr, ",") + ")"
}

func (m *Model) getSQLInsertArr() string {
	fieldArr, fieldArrKey, valuesArr := []string{}, []string{}, []string{}
	for k := range m.InsertArr[0] {
		fieldArr = append(fieldArr, k)
		fieldArrKey = append(fieldArrKey, k)
	}
	for _, value := range m.InsertArr {
		var valueArr []string
		for _, v := range fieldArrKey {
			m.Scan = append(m.Scan, value[v])
			placeholder := fmt.Sprintf("$%d", m.ParamsIndex)
			valueArr = append(valueArr, placeholder)
		}
		valuesArr = append(valuesArr, "("+strings.Join(valueArr, ",")+")")
		m.ParamsIndex++
	}
	return "(" + strings.Join(fieldArr, ",") + ")" + " values " + strings.Join(valuesArr, ",")
}

func (m *Model) getSQLLimite() string {
	if strArr, ok := m.Limit.([2]int); ok {
		return " LIMIT " + fmt.Sprintf("%d", strArr[0]) + ", " + fmt.Sprintf("%d", strArr[1])
	}
	if strArr, ok := m.Limit.([]int); ok {
		return " LIMIT " + fmt.Sprintf("%d", strArr[0]) + ", " + fmt.Sprintf("%d", strArr[1])
	}
	return ""
}

// Clean 清楚orm
func (m *Model) Clean() {
	m.Cond = nil
	m.Insert = nil
	m.InsertArr = nil
	m.Update = nil
	m.Field = ""
	m.Table = ""
	m.Index = ""
	m.Limit = nil
	m.Sort = ""
	m.GroupBy = ""
	m.IsDeug = 0
	m.ParamsIndex = 1
	m.Scan = []interface{}{}
}

// Debug 打印调试
func (m *Model) Debug(sql string) {
	if m.IsDeug == 1 {
		fmt.Println(sql, m.Scan)
	}
}

func logRecord(str string) {
	if OpenLog == 0 {
		return
	}
	log.Println(str)
}

func (m *Model) query(sqlStr string) ([]map[string]string, error) {
	m.Debug(sqlStr)
	var err error
	var rows pgx.Rows
	var resultsSlice []map[string]string
	if m.Tx == nil {
		rows, err = Db.Query(
			m.Ctx,
			sqlStr,
			m.Scan...,
		)
	} else {
		rows, err = m.Tx.Query(
			m.Ctx,
			sqlStr,
			m.Scan...,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions() // ★替代 Columns()
	if err != nil {
		logRecord("ERR:" + err.Error() + "SQL:" + sqlStr)
		return resultsSlice, err
	}

	for rows.Next() {
		result := make(map[string]string)
		var scanResultContainers []interface{}
		for i := 0; i < len(fields); i++ {
			var scanResultContainer interface{}
			scanResultContainers = append(scanResultContainers, &scanResultContainer)
		}
		if err := rows.Scan(scanResultContainers...); err != nil {
			return resultsSlice, err
		}
		for k, v := range fields {
			rawValue := reflect.Indirect(reflect.ValueOf(scanResultContainers[k]))
			if rawValue.Interface() == nil {
				result[v.Name] = ""
				continue
			}
			rawType := reflect.TypeOf(rawValue.Interface())
			rawVal := reflect.ValueOf(rawValue.Interface())
			var str string
			switch rawType.Kind() {
			case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				str = strconv.FormatInt(rawVal.Int(), 10)
				result[v.Name] = str
			case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				str = strconv.FormatUint(rawVal.Uint(), 10)
				result[v.Name] = str
			case reflect.Float32, reflect.Float64:
				str = strconv.FormatFloat(rawVal.Float(), 'f', -1, 64)
				result[v.Name] = str
			case reflect.Slice:
				if rawType.Elem().Kind() == reflect.Uint8 {
					result[v.Name] = string(rawVal.Interface().([]byte))
				}
			case reflect.String:
				str = rawVal.String()
				result[v.Name] = str
			case reflect.Struct:
				str = rawVal.Interface().(time.Time).Format("2006-01-02 15:04:05.000 -0700")
				result[v.Name] = str
			case reflect.Bool:
				if rawVal.Bool() {
					result[v.Name] = "1"
				} else {
					result[v.Name] = "0"
				}
			}
		}
		resultsSlice = append(resultsSlice, result)
	}
	return resultsSlice, nil
}
