package qesyPgSql

// ModelGet 获取一个Model
func ModelGet() Model {
	var m Model
	return m
}

// Select 查询
func Select(TableName string, Cond map[string]string) ([]map[string]string, error) {
	var m Model
	return m.SetTable(TableName).SetWhere(Cond).ExecSelect()
}

// SelectOne 查询一条
func SelectOne(TableName string, Cond map[string]string) (map[string]string, error) {
	var m Model
	return m.SetTable(TableName).SetWhere(Cond).ExecSelectOne()
}

// Insert 插入一条
func Insert(TableName string, InsertArr map[string]string) (string, error) {
	var m Model
	return m.SetTable(TableName).SetInsert(InsertArr).ExecInsert()
}

// InsertBatch 批量插入
func InsertBatch(TableName string, InsertArr []map[string]string) ([]string, error) {
	var m Model
	return m.SetTable(TableName).SetInsertArr(InsertArr).ExecInsertBatch()
}

// Replace 替换 (PgSql暂时不做)
// func Replace(TableName string, InsertArr map[string]string) (sql.Result, error) {
// 	var m Model
// 	return m.SetTable(TableName).SetInsert(InsertArr).ExecReplace()
// }

// Replace 批量替换 (PgSql暂时不做)
// func ReplaceBatch(TableName string, InsertArr []map[string]string) (sql.Result, error) {
// 	var m Model
// 	return m.SetTable(TableName).SetInsertArr(InsertArr).ExecReplaceBatch()
// }

// Update 修改
func Update(TableName string, UpdateArr map[string]string, Cond map[string]string) error {
	var m Model
	return m.SetTable(TableName).SetWhere(Cond).SetUpdate(UpdateArr).ExecUpdate()
}

// Delete 删除
func Delete(TableName string, Cond map[string]string) error {
	var m Model
	return m.SetTable(TableName).SetWhere(Cond).ExecDelete()
}
