package qesyPgSql

// SelectTx 查询
func SelectTx(TableName string, Cond map[string]string, m Model) ([]map[string]string, error) {
	return m.SetTable(TableName).SetWhere(Cond).ExecSelect()
}

// SelectOneTx 查询一条
func SelectOneTx(TableName string, Cond map[string]string, m Model) (map[string]string, error) {
	return m.SetTable(TableName).SetWhere(Cond).ExecSelectOne()
}

// InsertTx 插入
func InsertTx(TableName string, InsertArr map[string]string, PrimaryKey string, m Model) (string, error) {
	return m.SetTable(TableName).SetInsert(InsertArr).ExecInsert(PrimaryKey)
}

// InsertBatchTx 批量插入
func InsertBatchTx(TableName string, InsertArr []map[string]string, PrimaryKey string, m Model) ([]string, error) {
	return m.SetTable(TableName).SetInsertArr(InsertArr).ExecInsertBatch(PrimaryKey)
}

// ReplaceTx 替换
// func ReplaceTx(TableName string, InsertArr map[string]string, m Model) (sql.Result, error) {
// 	return m.SetTable(TableName).SetInsert(InsertArr).ExecReplace()
// }

// Replace 批量替换
// func ReplaceBatchTx(TableName string, InsertArr []map[string]string, m Model) (sql.Result, error) {
// 	return m.SetTable(TableName).SetInsertArr(InsertArr).ExecReplaceBatch()
// }

// UpdateTx 修改
func UpdateTx(TableName string, UpdateArr map[string]string, Cond map[string]string, m Model) error {
	return m.SetTable(TableName).SetWhere(Cond).SetUpdate(UpdateArr).ExecUpdate()
}

// DeleteTx 删除
func DeleteTx(TableName string, Cond map[string]string, m Model) error {
	return m.SetTable(TableName).SetWhere(Cond).ExecDelete()
}
