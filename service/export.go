package service

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/schema"
	"wea-recover/common"

	"strconv"
	"strings"
	"wea-recover/common/def"
	"wea-recover/mysql"
)

func Export(param def.InputInfo) error {
	err := checkExportParam(param)
	if err != nil {
		return err
	}

	// export
	return export(param)
}

func checkExportParam(param def.InputInfo) error {
	return nil
}

func export(param def.InputInfo) error {
	mysql.NewConnTestPool(mysql.DBConfig{
		Addr:     param.Addr + ":" + strconv.Itoa(param.Port),
		User:     param.User,
		Password: param.Pwd,
		DBName:   "test",
	})

	//重建恢复文件
	DeleteFile("./", param.Table+"_recover"+".sql")
	file, ef := CreateAndTruncate("./" + param.Table + "_recover" + ".sql")
	if ef != nil {
		return ef
	}
	defer file.Close()
	file.WriteString("use test;" + "\n")
	//链接测试库
	conn_src, err := client.Connect(param.Addr+":"+strconv.Itoa(param.Port), param.User, param.Pwd, "test")
	if err != nil {
		common.Errorln(fmt.Sprintf("connent export source db failed %v", err.Error()))
	}
	conn_src.SetCharset("utf8mb4")
	table, err := schema.NewTable(conn_src, "test", param.Table+"_recover")
	if err != nil {
		common.Errorln(fmt.Sprintf("get schema for %s export source db failed %v", param.Table+"_recover", err.Error()))
		return err
	}

	if len(table.PKColumns) != 0 {
		if table.GetPKColumn(0).Type != schema.TYPE_NUMBER {
			return fmt.Errorf("[%s]主键不是数值类型:", param.Table+"_recover")
		}
	}
	//获得主键最大最小值
	minId, me := getMinID(table.Name, table.GetPKColumn(0).Name)
	if me != me {
		return me
	}
	maxId, ae := getMaxID(table.Name, table.GetPKColumn(0).Name)
	if ae != nil {
		return ae
	}

	var fields []string
	var allFiels []string
	for _, field := range table.Columns {
		//是虚拟列,不用拼接到列名中
		if field.IsVirtual {
			allFiels = append(allFiels, fmt.Sprintf("`%s`", field.Name))
			continue
		}
		allFiels = append(allFiels, fmt.Sprintf("`%s`", field.Name))
		fields = append(fields, fmt.Sprintf("`%s`", field.Name))
	}
	fields_sql := "(" + strings.Join(fields, ",") + ")"
	pSql := fmt.Sprintf(`select %s from %s where %s > ? and %s <=?  order by %s limit %d`,
		strings.Join(fields, ","), table.Name, table.GetPKColumn(0).Name, table.GetPKColumn(0).Name,
		table.GetPKColumn(0).Name, param.PageSize)
	stmt, err := conn_src.Prepare(pSql)
	if err != nil {
		common.Errorln("执行PrepareSQL错误:%v, sql:%s]", err.Error(), pSql)
		return err
	} else {
		common.Errorln("执行PrepareSQL成功 sql:%s]", pSql)
	}

	nowID := minId - 1
	//查询主循环
	for nowID <= maxId {
		//common.Errorln("开始执行导出查询 nowID:%d, maxId:%d", nowID, maxId)
		ret, err := stmt.Execute(nowID, maxId)
		if err != nil {
			common.Errorln("执行[%s]的查询[%s]出现错误[%v]", table.Name, pSql, err.Error())
			return err
		}
		if len(ret.Values) == 0 {
			break
		}
		var oneSelect []string
		for row, cv := range ret.Values {
			var lineValue string
			var lineTmp []string
			for i, _ := range cv {
				val, e := ret.GetValue(row, i)
				if e != nil {
					return e
				}
				dataI, isNumber := mysql.ParseNumber(val)
				if isNumber {
					lineTmp = append(lineTmp, fmt.Sprintf("%v", dataI))
				} else {
					lineTmp = append(lineTmp, fmt.Sprintf("'%v'", dataI))
				}
				//value := mysql.ConvertToSqlValueString(v.AsString(), table.Columns[i].RawType)
				if table.Columns[i].Name == table.GetPKColumn(0).Name {
					if isNumber {
						switch pkv := dataI.(type) {
						//case int, int8, int16, int32, int64,
						//	uint, uint8, uint16, uint32, uint64, float32, float64:
						//	nowID = pkv.(int64)
						case int, int8, int16, int32, int64,
							uint, uint8, uint16, uint32, uint64:
							nowID, _ = strconv.ParseInt(fmt.Sprintf("%v", pkv), 10, 64)
						case float32, float64:
							return fmt.Errorf("float32, float64类型主键不支持")
						}
					} else {
						return fmt.Errorf("主键不是数值类型")
					}
					if err != nil {
						ret.Close()
						return err
					}
				}
			}
			lineValue = fmt.Sprintf("(%v)", strings.Join(lineTmp, ","))
			oneSelect = append(oneSelect, lineValue)
		}
		//写入文件
		sql := toSQL(param.Table, fields_sql, oneSelect)
		file.WriteString(sql + "\n")
		file.Sync()
		ret.Close()
	}

	return nil
}

func getMaxID(tName string, pk string) (int64, error) {
	sqlStr := fmt.Sprintf("select %s from %s  order by `%s` desc limit 1", pk, "test."+tName, pk)
	rsl := int64(0)
	err := mysql.QueryTestDBForRow(sqlStr, &rsl)
	if err != nil {
		return 0, common.Error(tName, "执行sql", sqlStr, "异常", err)
	}
	return rsl, err

}

func getMinID(tName string, pk string) (int64, error) {
	sqlStr := fmt.Sprintf("select %s from %s order by `%s`  limit 1", pk, "test."+tName, pk)

	rsl := int64(0)
	err := mysql.QueryTestDBForRow(sqlStr, &rsl)
	if err != nil {
		return 0, common.Error(tName, "执行sql", sqlStr, "异常", err)
	}
	return rsl, err

}

func toSQL(table string, field string, value []string) string {
	return fmt.Sprintf("replace into `%s` %s values %s", table, field, strings.Join(value, ","))
}
