package qesyPgSql

import "context"

func NewModel() Model {
	var m Model
	m.Ctx = context.Background()
	m.Tx = nil
	return m
}

// SetTable 选择数据表
func (m *Model) SetTable(Str string) *Model {
	m.Table = Str
	return m
}

// SetWhere 设置条件
func (m *Model) SetWhere(Cond interface{}) *Model {
	m.Cond = Cond
	return m
}

// SetInsert 设置插入字段
func (m *Model) SetInsert(InsertMap map[string]string) *Model {
	m.Insert = InsertMap
	return m
}

// SetInsertArr 设置批量插入字段
func (m *Model) SetInsertArr(InsertMapArr []map[string]string) *Model {
	m.InsertArr = InsertMapArr
	return m
}

// SetUpdate 设置修改字段
func (m *Model) SetUpdate(UpdateMap map[string]string) *Model {
	m.Update = UpdateMap
	return m
}

// SetField 设置查询字段
func (m *Model) SetField(Field string) *Model {
	m.Field = Field
	return m
}

// SetIndex 设置索引字段
func (m *Model) SetIndex(Index string) *Model {
	m.Index = Index
	return m
}

// SetLimit 设置查询数量
func (m *Model) SetLimit(Limit interface{}) *Model {
	m.Limit = Limit
	return m
}

// SetSort 设置排序字段
func (m *Model) SetSort(Sort string) *Model {
	m.Sort = Sort
	return m
}

// SetGroupBy 设置排序字段
func (m *Model) SetGroupBy(GroupBy string) *Model {
	m.GroupBy = GroupBy
	return m
}

// SetDebug 设置是否打开调试
func (m *Model) SetDebug(Debug int) *Model {
	m.IsDeug = Debug
	return m
}
