package mysql

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/gogf/gf/container/gpool"
	"strings"
	"time"
)

type SqlType int

const (
	SqlTypeUpdateOrInsert SqlType = iota
	SqlTypeDelete
	SqlTypeDDL
	SqlSafeFullStageInsert
	SqlSafeInsert
	SqlSafeUpdate
	SqlSafeDelete
)

var (
	NoTableInfoError                       = fmt.Errorf("no table schema")
	HandleUpdateRowsError                  = fmt.Errorf("handle update rows error")
	NoRowsDataToHandle                     = fmt.Errorf("nowRowsDataToHandle")
	HandleUpdateRowsTableFieldNumError     = fmt.Errorf("handle update rows table field number error")
	HandleUpdateRowsIndexesDontExistsError = fmt.Errorf("handle update rows indexs don't exists")
)

type HandType int

type SqlAttr struct {
	// 到目标的表名
	TName string
	// 原表名
	OTableName string
	Field      string
	PKValue    []string // 唯一键的值，这个值是为了后续处理可以校验是否可以并发执行，或者说是优化
	Value      []string
	HandType
	// 0 insert or update
	// 1 delete
	// 2 ddl
	SqlType    SqlType
	FieldCount int
	ByteCount  int
}

var sqlBuilder = gpool.New(time.Second*10, func() (interface{}, error) {
	return &strings.Builder{}, nil
}, func(i interface{}) {
	i.(*strings.Builder).Reset()
})

func (s *SqlAttr) Merge(a *SqlAttr) bool {
	// safeUpdate 不能合并
	if s.SqlType == SqlSafeUpdate || a.SqlType == SqlSafeUpdate {
		return false
	}
	// ddl 强制切换SQL
	if s.SqlType == SqlTypeDDL || a.SqlType == SqlTypeDDL {
		return false
	}
	if s.SqlType != a.SqlType || s.TName != a.TName || s.FieldCount != a.FieldCount {
		return false
	}
	if s.ByteCount+a.ByteCount > 15728640 {
		return false
	}
	// 最大合并200个列
	if len(s.Value) >= 200 {
		return false
	}
	// 字段名不一样
	if !strings.EqualFold(s.Field, a.Field) {
		return false
	}
	s.ByteCount += a.ByteCount
	s.Value = append(s.Value, a.Value...)
	s.PKValue = append(s.PKValue, a.PKValue...)
	return true
}

func (s *SqlAttr) ToSql() string {
	return fmt.Sprintf("replace into `test`.`%s` %s values %s", s.TName, s.Field, strings.Join(s.Value, ","))
}

func HandleUpdateEvent(rows [][]interface{}, table *schema.Table, sqlType SqlType) (sql []*SqlAttr, match bool, err error) {
	if table == nil {
		return nil, false, fmt.Errorf("%w", NoTableInfoError)
	}
	if len(rows)%2 != 0 {
		return nil, false, fmt.Errorf("%w", HandleUpdateRowsError)
	}
	if len(rows) == 0 {
		return nil, false, fmt.Errorf("%w", NoRowsDataToHandle)
	}
	poolB, err := sqlBuilder.Get()
	if err != nil {
		return nil, false, err
	}
	sqlB := poolB.(*strings.Builder)
	defer sqlBuilder.Put(sqlB)
	defer sqlB.Reset()

	rsl := []*SqlAttr{}
	// replace into 的逻辑
	if sqlType == SqlTypeUpdateOrInsert {
		sql := &SqlAttr{
			TName:      table.Name,
			OTableName: table.Name,
			SqlType:    sqlType,
			FieldCount: len(table.Columns),
			Field:      "",
			Value:      []string{},
			PKValue:    []string{},
		}
		haveField := false
		for i := 0; i < len(rows)/2; i++ {

			if !haveField {
				//使用前镜像进行恢复
				err := findRowAllFields(rows[2*i], table, sqlB)
				if err != nil {
					return nil, false, err
				}
				haveField = true
				sql.Field = sqlB.String()
				sqlB.Reset()
			}
			pk, err := findRowAllValues(rows[2*i], table, sqlB)
			if err != nil {
				return nil, false, err
			}
			sql.Value = append(sql.Value, sqlB.String())
			sql.PKValue = append(sql.PKValue, pk)
			sql.ByteCount += sqlB.Len()
			sqlB.Reset()
		}
		if len(sql.Value) != 0 {
			rsl = append(rsl, sql)
		}
	}
	return rsl, len(rsl) != 0, nil
}

func HandleDeleteEvent(rows [][]interface{}, table *schema.Table, sqlType SqlType) (sql *SqlAttr, match bool, err error) {
	if table == nil {
		return nil, false, fmt.Errorf("%w", NoTableInfoError)
	}
	if len(rows) == 0 {
		return nil, false, fmt.Errorf("%w", NoRowsDataToHandle)
	}

	poolB, err := sqlBuilder.Get()
	if err != nil {
		return nil, false, err
	}
	sqlB := poolB.(*strings.Builder)
	defer sqlBuilder.Put(sqlB)
	defer sqlB.Reset()

	sql = &SqlAttr{
		TName:      table.Name,
		OTableName: table.Name,
		SqlType:    sqlType,
		FieldCount: len(table.Columns),
		Field:      "",
		Value:      []string{},
		PKValue:    []string{},
	}

	for i := 0; i < len(rows); i++ {
		pk, err := findRowAllValues(rows[i], table, sqlB)
		if err != nil {
			return nil, false, err
		}
		sql.Value = append(sql.Value, sqlB.String())
		sql.PKValue = append(sql.PKValue, pk)
		sql.ByteCount += sqlB.Len()
		sqlB.Reset()
	}
	return sql, len(sql.Value) != 0, nil
}

func findRowToDelete(rows []interface{}, table *schema.Table, builder *strings.Builder) (pk string, err error) {

	builder.WriteString("(")

	for i, row := range rows {

		more := "'"
		if table.Columns[i].Type == schema.TYPE_NUMBER && table.Columns[i].IsUnsigned {
			row = signedToUnsigned(row)
		}
		value, _ := parseNumber(row)

		builder.WriteString(" and `")
		builder.WriteString(table.Columns[i].Name)
		builder.WriteString("`=")
		builder.WriteString(more)

		builder.WriteString(fmt.Sprintf("%v", value))
		pk = fmt.Sprintf("%v", value)

		builder.WriteString(more)

		break

	}
	builder.WriteString(")")
	return pk, nil
}

func findRowAllFields(rows []interface{}, table *schema.Table, builder *strings.Builder) (err error) {

	if len(rows) != len(table.Columns) {
		return HandleUpdateRowsTableFieldNumError
	}

	builder.WriteString("(")
	haveValue := false
	for _, col := range table.Columns {
		if col.IsVirtual {
			continue
		}
		if !haveValue {
			builder.WriteString("`")
			builder.WriteString(col.Name)
			builder.WriteString("`")
			haveValue = true
		} else {
			builder.WriteString(",`")
			builder.WriteString(col.Name)
			builder.WriteString("`")
		}
	}
	builder.WriteString(")")
	return nil
}

func findRowAllValues(cols []interface{}, table *schema.Table, builder *strings.Builder) (pk string, err error) {

	if len(cols) != len(table.Columns) {
		return "", HandleUpdateRowsTableFieldNumError
	}
	//if len(cols) > len(table.Columns) {
	//	proto.Infof("error: %s", HandleUpdateRowsTableFieldNumError.Error())
	//	return "", fmt.Errorf("%w", HandleUpdateRowsTableFieldNumError)
	//}

	builder.WriteString("(")
	haveValue := false

	for i, col := range cols {
		//虚拟列不需要组装再value值里面
		if table.Columns[i].IsVirtual {
			continue
		}

		more := "'"
		if table.Columns[i].Type == schema.TYPE_NUMBER && table.Columns[i].IsUnsigned {
			col = signedToUnsigned(col)
		}
		value, isNumber := parseNumber(col)
		if isNumber {
			more = ""
		}
		if !haveValue {
			builder.WriteString(more)
			builder.WriteString(fmt.Sprintf("%v", value))
			builder.WriteString(more)
			haveValue = true
		} else {
			builder.WriteString(",")
			builder.WriteString(more)
			builder.WriteString(fmt.Sprintf("%v", value))
			builder.WriteString(more)
		}
	}

	builder.WriteString(")")
	return pk, nil
}

func signedToUnsigned(v interface{}) interface{} {
	switch v.(type) {
	case int:
		return uint(v.(int))
	case int8:
		return uint8(v.(int8))
	case int16:
		return uint16(v.(int16))
	case int32:
		return uint32(v.(int32))
	case int64:
		return uint64(v.(int64))
	default:
		return v
	}
}
